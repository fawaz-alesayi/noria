Statement::Statement(
	Database* db,
	sqlite3_stmt* handle,
	sqlite3_uint64 id,
	bool returns_data
) :
	node::ObjectWrap(),
	db(db),
	handle(handle),
	extras(new Extras(id)),
	alive(true),
	locked(false),
	bound(false),
	has_bind_map(false),
	safe_ints(db->GetState()->safe_ints),
	mode(Data::FLAT),
	returns_data(returns_data) {
	assert(db != NULL);
	assert(handle != NULL);
	assert(db->GetState()->open);
	assert(!db->GetState()->busy);
	db->AddStatement(this);
}

Statement::~Statement() {
	if (alive) db->RemoveStatement(this);
	CloseHandles();
	delete extras;
}

// Whenever this is used, db->RemoveStatement must be invoked beforehand.
void Statement::CloseHandles() {
	if (alive) {
		alive = false;
		sqlite3_finalize(handle);
	}
}

// Returns the Statement's bind map (creates it upon first execution).
BindMap* Statement::GetBindMap(v8::Isolate* isolate) {
	if (has_bind_map) return &extras->bind_map;
	BindMap* bind_map = &extras->bind_map;
	int param_count = sqlite3_bind_parameter_count(handle);
	for (int i = 1; i <= param_count; ++i) {
		const char* name = sqlite3_bind_parameter_name(handle, i);
		if (name != NULL) bind_map->Add(isolate, name + 1, i);
	}
	has_bind_map = true;
	return bind_map;
}

Statement::Extras::Extras(sqlite3_uint64 id)
	: bind_map(0), id(id), noria_view_id(-1), column_names(),
	  stmt_type(Statement::STMT_OTHER), table_name(), table_id(-1) {}

INIT(Statement::Init) {
	v8::Local<v8::FunctionTemplate> t = NewConstructorTemplate(isolate, data, JS_new, "Statement");
	SetPrototypeMethod(isolate, data, t, "run", JS_run);
	SetPrototypeMethod(isolate, data, t, "get", JS_get);
	SetPrototypeMethod(isolate, data, t, "all", JS_all);
	SetPrototypeMethod(isolate, data, t, "getMany", JS_getMany);
	SetPrototypeMethod(isolate, data, t, "iterate", JS_iterate);
	SetPrototypeMethod(isolate, data, t, "bind", JS_bind);
	SetPrototypeMethod(isolate, data, t, "pluck", JS_pluck);
	SetPrototypeMethod(isolate, data, t, "expand", JS_expand);
	SetPrototypeMethod(isolate, data, t, "raw", JS_raw);
	SetPrototypeMethod(isolate, data, t, "safeIntegers", JS_safeIntegers);
	SetPrototypeMethod(isolate, data, t, "columns", JS_columns);
	SetPrototypeGetter(isolate, data, t, "busy", JS_busy);
	return t->GetFunction(OnlyContext).ToLocalChecked();
}

NODE_METHOD(Statement::JS_new) {
	UseAddon;
	if (!addon->privileged_info) {
		return ThrowTypeError("Statements can only be constructed by the db.prepare() method");
	}
	assert(info.IsConstructCall());
	Database* db = Unwrap<Database>(addon->privileged_info->This());
	REQUIRE_DATABASE_OPEN(db->GetState());
	REQUIRE_DATABASE_NOT_BUSY(db->GetState());

	v8::Local<v8::String> source = (*addon->privileged_info)[0].As<v8::String>();
	v8::Local<v8::Object> database = (*addon->privileged_info)[1].As<v8::Object>();
	bool pragmaMode = (*addon->privileged_info)[2].As<v8::Boolean>()->Value();
	int flags = SQLITE_PREPARE_PERSISTENT;

	if (pragmaMode) {
		REQUIRE_DATABASE_NO_ITERATORS_UNLESS_UNSAFE(db->GetState());
		flags = 0;
	}

	UseIsolate;
	v8::String::Utf8Value utf8(isolate, source);
	sqlite3_stmt* handle;
	const char* tail;

	if (sqlite3_prepare_v3(db->GetHandle(), *utf8, utf8.length() + 1, flags, &handle, &tail) != SQLITE_OK) {
		return db->ThrowDatabaseError();
	}
	if (handle == NULL) {
		return ThrowRangeError("The supplied SQL string contains no statements");
	}
	// https://github.com/WiseLibs/better-sqlite3/issues/975#issuecomment-1520934678
	for (char c; (c = *tail); ) {
		if (IS_SKIPPED(c)) {
			++tail;
			continue;
		}
		if (c == '/' && tail[1] == '*') {
			tail += 2;
			for (char c; (c = *tail); ++tail) {
				if (c == '*' && tail[1] == '/') {
					tail += 2;
					break;
				}
			}
		} else if (c == '-' && tail[1] == '-') {
			tail += 2;
			for (char c; (c = *tail); ++tail) {
				if (c == '\n') {
					++tail;
					break;
				}
			}
		} else {
			sqlite3_finalize(handle);
			return ThrowRangeError("The supplied SQL string contains more than one statement");
		}
	}

	UseContext;
	bool returns_data = sqlite3_column_count(handle) >= 1 || pragmaMode;
	Statement* stmt = new Statement(db, handle, addon->NextId(), returns_data);
	stmt->Wrap(info.This());
	SetFrozen(isolate, ctx, info.This(), addon->cs.reader, v8::Boolean::New(isolate, returns_data));
	SetFrozen(isolate, ctx, info.This(), addon->cs.readonly, v8::Boolean::New(isolate, sqlite3_stmt_readonly(handle) != 0));
	SetFrozen(isolate, ctx, info.This(), addon->cs.source, source);
	SetFrozen(isolate, ctx, info.This(), addon->cs.database, database);

	// Try to register as a Noria view for acceleration
	if (returns_data && !pragmaMode) {
		stmt->TryRegisterNoriaView(isolate);
	}

	// For write statements, detect type and table for CDC
	if (!returns_data) {
		stmt->DetectStatementType();
	}

	info.GetReturnValue().Set(info.This());
}

NODE_METHOD(Statement::JS_run) {
	STATEMENT_START(ALLOW_ANY_STATEMENT, DOES_MUTATE);
	sqlite3* db_handle = db->GetHandle();
	int total_changes_before = sqlite3_total_changes(db_handle);
	int autocommit_before = sqlite3_get_autocommit(db_handle);

	sqlite3_step(handle);
	if (sqlite3_reset(handle) == SQLITE_OK) {
		int changes = sqlite3_total_changes(db_handle) == total_changes_before ? 0 : sqlite3_changes(db_handle);
		sqlite3_int64 id = sqlite3_last_insert_rowid(db_handle);
		int autocommit_after = sqlite3_get_autocommit(db_handle);

		// Notify Noria of changes for cache invalidation
		// Fast path: skip if no views registered (table_id == -2 means "checked, no views")
		// table_id == -1 means "not checked yet", >= 0 means "has views"
		if (changes > 0 && stmt->extras->table_id != -2 &&
		    stmt->extras->stmt_type >= Statement::STMT_INSERT &&
		    stmt->extras->stmt_type <= Statement::STMT_DELETE) {
			stmt->NotifyCdc();
		}
		// Also notify after COMMIT (autocommit transitions 0 -> 1)
		// This processes accumulated CDC events from the committed transaction
		else if (autocommit_before == 0 && autocommit_after == 1) {
			stmt->NotifyCdc();
		}

		Addon* addon = db->GetAddon();
		UseContext;
		v8::Local<v8::Object> result = v8::Object::New(isolate);
		result->Set(ctx, addon->cs.changes.Get(isolate), v8::Int32::New(isolate, changes)).FromJust();
		result->Set(ctx, addon->cs.lastInsertRowid.Get(isolate),
			stmt->safe_ints
				? v8::BigInt::New(isolate, id).As<v8::Value>()
				: v8::Number::New(isolate, (double)id).As<v8::Value>()
		).FromJust();
		STATEMENT_RETURN(result);
	}
	STATEMENT_THROW();
}

NODE_METHOD(Statement::JS_get) {
	STATEMENT_START(REQUIRE_STATEMENT_RETURNS_DATA, DOES_NOT_MUTATE);

	// Try Noria cache first (after parameters are bound)
	if (stmt->TryNoriaGet(isolate, info, false)) {
		if (!bound) { sqlite3_clear_bindings(handle); }
		db->GetState()->busy = false;
		return;  // TryNoriaGet set the return value
	}

	int status = sqlite3_step(handle);
	if (status == SQLITE_ROW) {
		v8::Local<v8::Value> result = Data::GetRowJS(isolate, OnlyContext, handle, stmt->safe_ints, stmt->mode);
		sqlite3_reset(handle);
		STATEMENT_RETURN(result);
	} else if (status == SQLITE_DONE) {
		sqlite3_reset(handle);
		STATEMENT_RETURN(v8::Undefined(isolate));
	}
	sqlite3_reset(handle);
	STATEMENT_THROW();
}

NODE_METHOD(Statement::JS_all) {
	STATEMENT_START(REQUIRE_STATEMENT_RETURNS_DATA, DOES_NOT_MUTATE);
	UseContext;
	const bool safe_ints = stmt->safe_ints;
	const char mode = stmt->mode;

#if !defined(NODE_MODULE_VERSION) || NODE_MODULE_VERSION < 127
	bool js_error = false;
	uint32_t row_count = 0;
	v8::Local<v8::Array> result = v8::Array::New(isolate, 0);

	while (sqlite3_step(handle) == SQLITE_ROW) {
		if (row_count == 0xffffffff) { ThrowRangeError("Array overflow (too many rows returned)"); js_error = true; break; }
		result->Set(ctx, row_count++, Data::GetRowJS(isolate, ctx, handle, safe_ints, mode)).FromJust();
	}

	if (sqlite3_reset(handle) == SQLITE_OK && !js_error) {
		STATEMENT_RETURN(result);
	}
	if (js_error) db->GetState()->was_js_error = true;
	STATEMENT_THROW();
#else
	v8::LocalVector<v8::Value> rows(isolate);
	rows.reserve(8);

	if (mode == Data::FLAT) {
		RowBuilder rowBuilder(isolate, handle, safe_ints);
		while (sqlite3_step(handle) == SQLITE_ROW) {
			rows.emplace_back(rowBuilder.GetRowJS());
		}
	} else {
		while (sqlite3_step(handle) == SQLITE_ROW) {
			rows.emplace_back(Data::GetRowJS(isolate, ctx, handle, safe_ints, mode));
		}
	}

	if (sqlite3_reset(handle) == SQLITE_OK) {
		if (rows.size() > 0xffffffff) {
			ThrowRangeError("Array overflow (too many rows returned)");
			db->GetState()->was_js_error = true;
		} else {
			STATEMENT_RETURN(v8::Array::New(isolate, rows.data(), rows.size()));
		}
	}
	STATEMENT_THROW();
#endif
}

NODE_METHOD(Statement::JS_iterate) {
	UseAddon;
	UseIsolate;
	v8::Local<v8::Function> c = addon->StatementIterator.Get(isolate);
	addon->privileged_info = &info;
	v8::MaybeLocal<v8::Object> maybeIterator = c->NewInstance(OnlyContext, 0, NULL);
	addon->privileged_info = NULL;
	if (!maybeIterator.IsEmpty()) info.GetReturnValue().Set(maybeIterator.ToLocalChecked());
}

NODE_METHOD(Statement::JS_bind) {
	Statement* stmt = Unwrap<Statement>(info.This());
	if (stmt->bound) return ThrowTypeError("The bind() method can only be invoked once per statement object");
	REQUIRE_DATABASE_OPEN(stmt->db->GetState());
	REQUIRE_DATABASE_NOT_BUSY(stmt->db->GetState());
	REQUIRE_STATEMENT_NOT_LOCKED(stmt);
	STATEMENT_BIND(stmt->handle);
	stmt->bound = true;
	info.GetReturnValue().Set(info.This());
}

NODE_METHOD(Statement::JS_pluck) {
	Statement* stmt = Unwrap<Statement>(info.This());
	if (!stmt->returns_data) return ThrowTypeError("The pluck() method is only for statements that return data");
	REQUIRE_DATABASE_NOT_BUSY(stmt->db->GetState());
	REQUIRE_STATEMENT_NOT_LOCKED(stmt);
	bool use = true;
	if (info.Length() != 0) { REQUIRE_ARGUMENT_BOOLEAN(first, use); }
	stmt->mode = use ? Data::PLUCK : stmt->mode == Data::PLUCK ? Data::FLAT : stmt->mode;
	info.GetReturnValue().Set(info.This());
}

NODE_METHOD(Statement::JS_expand) {
	Statement* stmt = Unwrap<Statement>(info.This());
	if (!stmt->returns_data) return ThrowTypeError("The expand() method is only for statements that return data");
	REQUIRE_DATABASE_NOT_BUSY(stmt->db->GetState());
	REQUIRE_STATEMENT_NOT_LOCKED(stmt);
	bool use = true;
	if (info.Length() != 0) { REQUIRE_ARGUMENT_BOOLEAN(first, use); }
	stmt->mode = use ? Data::EXPAND : stmt->mode == Data::EXPAND ? Data::FLAT : stmt->mode;
	info.GetReturnValue().Set(info.This());
}

NODE_METHOD(Statement::JS_raw) {
	Statement* stmt = Unwrap<Statement>(info.This());
	if (!stmt->returns_data) return ThrowTypeError("The raw() method is only for statements that return data");
	REQUIRE_DATABASE_NOT_BUSY(stmt->db->GetState());
	REQUIRE_STATEMENT_NOT_LOCKED(stmt);
	bool use = true;
	if (info.Length() != 0) { REQUIRE_ARGUMENT_BOOLEAN(first, use); }
	stmt->mode = use ? Data::RAW : stmt->mode == Data::RAW ? Data::FLAT : stmt->mode;
	info.GetReturnValue().Set(info.This());
}

NODE_METHOD(Statement::JS_safeIntegers) {
	Statement* stmt = Unwrap<Statement>(info.This());
	REQUIRE_DATABASE_NOT_BUSY(stmt->db->GetState());
	REQUIRE_STATEMENT_NOT_LOCKED(stmt);
	if (info.Length() == 0) stmt->safe_ints = true;
	else { REQUIRE_ARGUMENT_BOOLEAN(first, stmt->safe_ints); }
	info.GetReturnValue().Set(info.This());
}

NODE_METHOD(Statement::JS_columns) {
	Statement* stmt = Unwrap<Statement>(info.This());
	if (!stmt->returns_data) return ThrowTypeError("The columns() method is only for statements that return data");
	REQUIRE_DATABASE_OPEN(stmt->db->GetState());
	REQUIRE_DATABASE_NOT_BUSY(stmt->db->GetState());
	Addon* addon = stmt->db->GetAddon();
	UseIsolate;

#if !defined(NODE_MODULE_VERSION) || NODE_MODULE_VERSION < 127
	UseContext;
	int column_count = sqlite3_column_count(stmt->handle);
	v8::Local<v8::Array> columns = v8::Array::New(isolate);

	v8::Local<v8::String> name = addon->cs.name.Get(isolate);
	v8::Local<v8::String> columnName = addon->cs.column.Get(isolate);
	v8::Local<v8::String> tableName = addon->cs.table.Get(isolate);
	v8::Local<v8::String> databaseName = addon->cs.database.Get(isolate);
	v8::Local<v8::String> typeName = addon->cs.type.Get(isolate);

	for (int i = 0; i < column_count; ++i) {
		v8::Local<v8::Object> column = v8::Object::New(isolate);

		column->Set(ctx, name,
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_name(stmt->handle, i), -1)
		).FromJust();
		column->Set(ctx, columnName,
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_origin_name(stmt->handle, i), -1)
		).FromJust();
		column->Set(ctx, tableName,
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_table_name(stmt->handle, i), -1)
		).FromJust();
		column->Set(ctx, databaseName,
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_database_name(stmt->handle, i), -1)
		).FromJust();
		column->Set(ctx, typeName,
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_decltype(stmt->handle, i), -1)
		).FromJust();

		columns->Set(ctx, i, column).FromJust();
	}

	info.GetReturnValue().Set(columns);
#else
	v8::LocalVector<v8::Name> keys(isolate);
	keys.reserve(5);
	keys.emplace_back(addon->cs.name.Get(isolate).As<v8::Name>());
	keys.emplace_back(addon->cs.column.Get(isolate).As<v8::Name>());
	keys.emplace_back(addon->cs.table.Get(isolate).As<v8::Name>());
	keys.emplace_back(addon->cs.database.Get(isolate).As<v8::Name>());
	keys.emplace_back(addon->cs.type.Get(isolate).As<v8::Name>());

	int column_count = sqlite3_column_count(stmt->handle);
	v8::LocalVector<v8::Value> columns(isolate);
	columns.reserve(column_count);

	for (int i = 0; i < column_count; ++i) {
		v8::LocalVector<v8::Value> values(isolate);
		keys.reserve(5);
		values.emplace_back(
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_name(stmt->handle, i), -1)
		);
		values.emplace_back(
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_origin_name(stmt->handle, i), -1)
		);
		values.emplace_back(
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_table_name(stmt->handle, i), -1)
		);
		values.emplace_back(
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_database_name(stmt->handle, i), -1)
		);
		values.emplace_back(
			InternalizedFromUtf8OrNull(isolate, sqlite3_column_decltype(stmt->handle, i), -1)
		);
		columns.emplace_back(
			v8::Object::New(isolate,
				GET_PROTOTYPE(v8::Object::New(isolate)),
				keys.data(),
				values.data(),
				keys.size()
			)
		);
	}

	info.GetReturnValue().Set(
		v8::Array::New(isolate, columns.data(), columns.size())
	);
#endif
}

// Try to register this statement as a Noria view
void Statement::TryRegisterNoriaView(v8::Isolate* isolate) {
	if (!returns_data) return;  // Only SELECT statements
	if (extras->noria_view_id >= 0) return;  // Already registered

	Noria* noria = db->GetNoria();
	if (!noria || !noria->IsEnabled()) return;

	const char* sql = sqlite3_sql(handle);
	if (!sql) return;

	// Try to register the view
	int view_id = noria->RegisterView(sql);
	if (view_id >= 0) {
		extras->noria_view_id = view_id;
		CacheColumnNames(isolate);
	}
}

// Build column names cache for Noria row construction
void Statement::CacheColumnNames(v8::Isolate* isolate) {
	int column_count = sqlite3_column_count(handle);
	extras->column_names.clear();
	extras->column_names.reserve(column_count);

	for (int i = 0; i < column_count; ++i) {
		const char* name = sqlite3_column_name(handle, i);
		v8::Local<v8::Name> key = InternalizedFromUtf8(isolate, name, -1).As<v8::Name>();
		extras->column_names.emplace_back(isolate, key);
	}
}

// Convert V8 value to NoriaValue for cache lookup
static NoriaValue V8ToNoriaKey(v8::Isolate* isolate, v8::Local<v8::Value> value, std::string& str_storage) {
	NoriaValue nv;
	memset(&nv, 0, sizeof(nv));

	if (value->IsNull() || value->IsUndefined()) {
		nv.value_type = NORIA_NULL;
	} else if (value->IsNumber()) {
		double d = value.As<v8::Number>()->Value();
		if (d == static_cast<double>(static_cast<int64_t>(d))) {
			nv.value_type = NORIA_INTEGER;
			nv.int_value = static_cast<int64_t>(d);
		} else {
			nv.value_type = NORIA_FLOAT;
			nv.float_value = d;
		}
	} else if (value->IsBigInt()) {
		bool lossless;
		nv.value_type = NORIA_INTEGER;
		nv.int_value = value.As<v8::BigInt>()->Int64Value(&lossless);
	} else if (value->IsString()) {
		v8::String::Utf8Value utf8(isolate, value.As<v8::String>());
		str_storage = std::string(*utf8, utf8.length());
		nv.value_type = NORIA_TEXT;
		nv.text_ptr = str_storage.c_str();
		nv.text_len = static_cast<int>(str_storage.length());
	} else if (node::Buffer::HasInstance(value)) {
		nv.value_type = NORIA_BLOB;
		nv.blob_ptr = reinterpret_cast<const uint8_t*>(node::Buffer::Data(value));
		nv.blob_len = static_cast<int>(node::Buffer::Length(value));
	}

	return nv;
}

// Attempt to serve get() from Noria cache
bool Statement::TryNoriaGet(v8::Isolate* isolate, const v8::FunctionCallbackInfo<v8::Value>& info, bool consistent_read) {
	if (extras->noria_view_id < 0) return false;  // No view registered
	if (mode != Data::FLAT) return false;  // Only support FLAT mode for now

	Noria* noria = db->GetNoria();
	if (!noria || !noria->IsEnabled()) return false;

	// If consistent read requested, flush pending CDC events first
	if (consistent_read) {
		noria->Flush();
	}

	// Extract parameters from info args as Noria keys
	int param_count = sqlite3_bind_parameter_count(handle);
	std::vector<NoriaValue> keys(param_count);
	std::vector<std::string> string_storage(param_count);  // Keep strings alive

	// Extract values from info - handles positional args and flattened arrays
	int info_index = 0;
	for (int i = 0; i < param_count && info_index < info.Length(); ++i) {
		v8::Local<v8::Value> arg = info[info_index];

		// Handle array argument - flatten it
		if (arg->IsArray()) {
			v8::Local<v8::Array> arr = arg.As<v8::Array>();
			for (uint32_t j = 0; j < arr->Length() && i < param_count; ++j, ++i) {
				v8::Local<v8::Value> elem = arr->Get(isolate->GetCurrentContext(), j).ToLocalChecked();
				keys[i] = V8ToNoriaKey(isolate, elem, string_storage[i]);
			}
			--i;  // Will be incremented by loop
		} else {
			keys[i] = V8ToNoriaKey(isolate, arg, string_storage[i]);
		}
		++info_index;
	}

	// Try cache lookup with upquery fallback
	NoriaLookupResult result = noria->LookupOrUpquery(extras->noria_view_id, keys.data(), param_count);

	if (!result.found && result.row_count == 0) {
		// Cache miss with no fallback result - let SQLite handle it
		return false;
	}

	if (result.row_count == 0) {
		// Empty result from cache
		info.GetReturnValue().Set(v8::Undefined(isolate));
		Noria::FreeRows(result.rows);
		return true;
	}

	// Build JS row from first result
	std::vector<v8::Local<v8::Name>> col_names;
	col_names.reserve(extras->column_names.size());
	for (const auto& global : extras->column_names) {
		col_names.push_back(global.Get(isolate));
	}

	v8::Local<v8::Value> row = NoriaRowToJS(isolate, result.rows, 0, col_names, safe_ints);
	info.GetReturnValue().Set(row);
	Noria::FreeRows(result.rows);
	return true;
}

// Attempt to serve all() from Noria cache
bool Statement::TryNoriaAll(v8::Isolate* isolate, const v8::FunctionCallbackInfo<v8::Value>& info, bool consistent_read) {
	if (extras->noria_view_id < 0) return false;  // No view registered
	if (mode != Data::FLAT) return false;  // Only support FLAT mode for now

	Noria* noria = db->GetNoria();
	if (!noria || !noria->IsEnabled()) return false;

	// If consistent read requested, flush pending CDC events first
	if (consistent_read) {
		noria->Flush();
	}

	// Extract bound parameters as Noria keys
	int param_count = sqlite3_bind_parameter_count(handle);
	std::vector<NoriaValue> keys(param_count);

	// Try cache lookup with upquery fallback
	NoriaLookupResult result = noria->LookupOrUpquery(extras->noria_view_id, keys.data(), param_count);

	if (!result.found && result.row_count == 0) {
		// Cache miss with no fallback result - let SQLite handle it
		return false;
	}

	// Build JS array from results
	std::vector<v8::Local<v8::Name>> col_names;
	col_names.reserve(extras->column_names.size());
	for (const auto& global : extras->column_names) {
		col_names.push_back(global.Get(isolate));
	}

#if defined(NODE_MODULE_VERSION) && NODE_MODULE_VERSION >= 127
	v8::LocalVector<v8::Value> rows(isolate);
	rows.reserve(result.row_count);

	for (int i = 0; i < result.row_count; ++i) {
		rows.emplace_back(NoriaRowToJS(isolate, result.rows, i, col_names, safe_ints));
	}

	info.GetReturnValue().Set(v8::Array::New(isolate, rows.data(), rows.size()));
#else
	v8::Local<v8::Context> ctx = isolate->GetCurrentContext();
	v8::Local<v8::Array> array = v8::Array::New(isolate, result.row_count);

	for (int i = 0; i < result.row_count; ++i) {
		array->Set(ctx, i, NoriaRowToJS(isolate, result.rows, i, col_names, safe_ints)).FromJust();
	}

	info.GetReturnValue().Set(array);
#endif

	Noria::FreeRows(result.rows);
	return true;
}

// Attempt to serve getMany() from Noria cache - batch lookup for multiple keys
// Takes an array of keys and returns an array of results, one per key
bool Statement::TryNoriaGetMany(v8::Isolate* isolate, const v8::FunctionCallbackInfo<v8::Value>& info, bool consistent_read) {
	if (extras->noria_view_id < 0) return false;  // No view registered
	if (mode != Data::FLAT) return false;  // Only support FLAT mode for now

	Noria* noria = db->GetNoria();
	if (!noria || !noria->IsEnabled()) return false;

	// First argument must be an array of keys
	if (info.Length() < 1 || !info[0]->IsArray()) {
		return false;
	}

	v8::Local<v8::Array> keys_array = info[0].As<v8::Array>();
	int num_keys = keys_array->Length();
	if (num_keys == 0) {
		// Return empty array for empty input
		info.GetReturnValue().Set(v8::Array::New(isolate, 0));
		return true;
	}

	// If consistent read requested, flush pending CDC events first
	if (consistent_read) {
		noria->Flush();
	}

	int param_count = sqlite3_bind_parameter_count(handle);

	// Prepare key arrays for batch lookup
	std::vector<std::vector<NoriaValue>> all_keys(num_keys);
	std::vector<std::vector<std::string>> all_string_storage(num_keys);  // Keep strings alive
	std::vector<const NoriaValue*> key_ptrs(num_keys);
	std::vector<int> key_counts(num_keys, param_count);

	v8::Local<v8::Context> ctx = isolate->GetCurrentContext();

	// Convert each key from JavaScript to NoriaValue
	for (int i = 0; i < num_keys; ++i) {
		all_keys[i].resize(param_count);
		all_string_storage[i].resize(param_count);

		v8::Local<v8::Value> key_val = keys_array->Get(ctx, i).ToLocalChecked();

		// Handle array key (multiple columns) or single value key
		if (key_val->IsArray()) {
			v8::Local<v8::Array> key_arr = key_val.As<v8::Array>();
			for (int j = 0; j < param_count && j < (int)key_arr->Length(); ++j) {
				v8::Local<v8::Value> elem = key_arr->Get(ctx, j).ToLocalChecked();
				all_keys[i][j] = V8ToNoriaKey(isolate, elem, all_string_storage[i][j]);
			}
		} else {
			// Single value key
			all_keys[i][0] = V8ToNoriaKey(isolate, key_val, all_string_storage[i][0]);
		}

		key_ptrs[i] = all_keys[i].data();
	}

	// Perform batch lookup
	NoriaBatchLookupResult batch_result = noria->LookupBatch(
		extras->noria_view_id,
		key_ptrs.data(),
		key_counts.data(),
		num_keys
	);

	if (batch_result.count == 0 || !batch_result.results) {
		// Batch lookup failed - fall back to SQLite
		return false;
	}

	// Build column names cache
	std::vector<v8::Local<v8::Name>> col_names;
	col_names.reserve(extras->column_names.size());
	for (const auto& global : extras->column_names) {
		col_names.push_back(global.Get(isolate));
	}

	// First pass: check if all keys hit the cache
	// If any key misses, fall back to SQLite (which does upqueries)
	bool all_found = true;
	for (int i = 0; i < num_keys; ++i) {
		int found, row_count;
		Noria::BatchGetResult(batch_result.results, i, &found, &row_count);
		if (!found) {
			all_found = false;
			break;
		}
	}

	if (!all_found) {
		// Some keys missed - fall back to SQLite for consistent results
		Noria::FreeBatchResults(batch_result.results);
		return false;
	}

	// All keys hit - build result array
#if defined(NODE_MODULE_VERSION) && NODE_MODULE_VERSION >= 127
	v8::LocalVector<v8::Value> results(isolate);
	results.reserve(num_keys);

	for (int i = 0; i < num_keys; ++i) {
		int found, row_count;
		void* rows_ptr = Noria::BatchGetResult(batch_result.results, i, &found, &row_count);

		if (row_count == 0) {
			// Empty result (key exists but no rows)
			results.emplace_back(v8::Undefined(isolate));
		} else {
			// Return first row (like get())
			results.emplace_back(NoriaRowToJS(isolate, rows_ptr, 0, col_names, safe_ints));
		}
	}

	info.GetReturnValue().Set(v8::Array::New(isolate, results.data(), results.size()));
#else
	v8::Local<v8::Array> result_array = v8::Array::New(isolate, num_keys);

	for (int i = 0; i < num_keys; ++i) {
		int found, row_count;
		void* rows_ptr = Noria::BatchGetResult(batch_result.results, i, &found, &row_count);

		if (row_count == 0) {
			result_array->Set(ctx, i, v8::Undefined(isolate)).FromJust();
		} else {
			result_array->Set(ctx, i, NoriaRowToJS(isolate, rows_ptr, 0, col_names, safe_ints)).FromJust();
		}
	}

	info.GetReturnValue().Set(result_array);
#endif

	Noria::FreeBatchResults(batch_result.results);
	return true;
}

// JS method: stmt.getMany([key1, key2, key3, ...]) -> [row1, row2, row3, ...]
// Performs batch lookup in Noria cache, falling back to SQLite for cache misses
NODE_METHOD(Statement::JS_getMany) {
	Statement* stmt = Unwrap<Statement>(info.This());
	if (!stmt->returns_data) {
		return ThrowTypeError("This statement does not return data. Use run() instead");
	}
	sqlite3_stmt* handle = stmt->handle;
	Database* db = stmt->db;
	REQUIRE_DATABASE_OPEN(db->GetState());
	REQUIRE_DATABASE_NOT_BUSY(db->GetState());
	REQUIRE_STATEMENT_NOT_LOCKED(stmt);
	UseIsolate;
	UseContext;

	// Validate input
	if (info.Length() < 1 || !info[0]->IsArray()) {
		return ThrowTypeError("Expected an array of keys");
	}

	v8::Local<v8::Array> keys_array = info[0].As<v8::Array>();
	uint32_t num_keys = keys_array->Length();

	if (num_keys == 0) {
		info.GetReturnValue().Set(v8::Array::New(isolate, 0));
		return;
	}

	// Try Noria cache first with batch lookup
	if (stmt->TryNoriaGetMany(isolate, info, false)) {
		return;  // TryNoriaGetMany set the return value
	}

	// Fall back to individual SQLite queries
	db->GetState()->busy = true;

#if defined(NODE_MODULE_VERSION) && NODE_MODULE_VERSION >= 127
	v8::LocalVector<v8::Value> results(isolate);
	results.reserve(num_keys);

	for (uint32_t i = 0; i < num_keys; ++i) {
		v8::Local<v8::Value> key_val = keys_array->Get(ctx, i).ToLocalChecked();

		// Reset and bind
		sqlite3_reset(handle);
		sqlite3_clear_bindings(handle);

		// Bind the key value(s)
		if (key_val->IsInt32()) {
			sqlite3_bind_int(handle, 1, key_val.As<v8::Int32>()->Value());
		} else if (key_val->IsNumber()) {
			sqlite3_bind_double(handle, 1, key_val.As<v8::Number>()->Value());
		} else if (key_val->IsString()) {
			v8::String::Utf8Value str(isolate, key_val);
			sqlite3_bind_text(handle, 1, *str, str.length(), SQLITE_TRANSIENT);
		} else if (key_val->IsArray()) {
			v8::Local<v8::Array> key_arr = key_val.As<v8::Array>();
			for (uint32_t j = 0; j < key_arr->Length(); ++j) {
				v8::Local<v8::Value> elem = key_arr->Get(ctx, j).ToLocalChecked();
				if (elem->IsInt32()) {
					sqlite3_bind_int(handle, j + 1, elem.As<v8::Int32>()->Value());
				} else if (elem->IsNumber()) {
					sqlite3_bind_double(handle, j + 1, elem.As<v8::Number>()->Value());
				} else if (elem->IsString()) {
					v8::String::Utf8Value str(isolate, elem);
					sqlite3_bind_text(handle, j + 1, *str, str.length(), SQLITE_TRANSIENT);
				} else {
					sqlite3_bind_null(handle, j + 1);
				}
			}
		} else {
			sqlite3_bind_null(handle, 1);
		}

		int status = sqlite3_step(handle);
		if (status == SQLITE_ROW) {
			results.emplace_back(Data::GetRowJS(isolate, ctx, handle, stmt->safe_ints, stmt->mode));
		} else {
			results.emplace_back(v8::Undefined(isolate));
		}
	}

	sqlite3_reset(handle);
	sqlite3_clear_bindings(handle);
	db->GetState()->busy = false;
	info.GetReturnValue().Set(v8::Array::New(isolate, results.data(), results.size()));
#else
	v8::Local<v8::Array> result_array = v8::Array::New(isolate, num_keys);

	for (uint32_t i = 0; i < num_keys; ++i) {
		v8::Local<v8::Value> key_val = keys_array->Get(ctx, i).ToLocalChecked();

		sqlite3_reset(handle);
		sqlite3_clear_bindings(handle);

		// Bind the key value(s)
		if (key_val->IsInt32()) {
			sqlite3_bind_int(handle, 1, key_val.As<v8::Int32>()->Value());
		} else if (key_val->IsNumber()) {
			sqlite3_bind_double(handle, 1, key_val.As<v8::Number>()->Value());
		} else if (key_val->IsString()) {
			v8::String::Utf8Value str(isolate, key_val);
			sqlite3_bind_text(handle, 1, *str, str.length(), SQLITE_TRANSIENT);
		} else if (key_val->IsArray()) {
			v8::Local<v8::Array> key_arr = key_val.As<v8::Array>();
			for (uint32_t j = 0; j < key_arr->Length(); ++j) {
				v8::Local<v8::Value> elem = key_arr->Get(ctx, j).ToLocalChecked();
				if (elem->IsInt32()) {
					sqlite3_bind_int(handle, j + 1, elem.As<v8::Int32>()->Value());
				} else if (elem->IsNumber()) {
					sqlite3_bind_double(handle, j + 1, elem.As<v8::Number>()->Value());
				} else if (elem->IsString()) {
					v8::String::Utf8Value str(isolate, elem);
					sqlite3_bind_text(handle, j + 1, *str, str.length(), SQLITE_TRANSIENT);
				} else {
					sqlite3_bind_null(handle, j + 1);
				}
			}
		} else {
			sqlite3_bind_null(handle, 1);
		}

		int status = sqlite3_step(handle);
		if (status == SQLITE_ROW) {
			result_array->Set(ctx, i, Data::GetRowJS(isolate, ctx, handle, stmt->safe_ints, stmt->mode)).FromJust();
		} else {
			result_array->Set(ctx, i, v8::Undefined(isolate)).FromJust();
		}
	}

	sqlite3_reset(handle);
	sqlite3_clear_bindings(handle);
	db->GetState()->busy = false;
	info.GetReturnValue().Set(result_array);
#endif
}

NODE_GETTER(Statement::JS_busy) {
	Statement* stmt = Unwrap<Statement>(info.This());
	info.GetReturnValue().Set(stmt->alive && stmt->locked);
}

// Helper to find case-insensitive substring
static const char* FindCaseInsensitive(const char* haystack, const char* needle) {
	size_t needle_len = strlen(needle);
	while (*haystack) {
		if (strncasecmp(haystack, needle, needle_len) == 0) {
			return haystack;
		}
		haystack++;
	}
	return nullptr;
}

// Detect statement type and extract table name for CDC
void Statement::DetectStatementType() {
	const char* sql = sqlite3_sql(handle);
	if (!sql) {
		extras->stmt_type = STMT_OTHER;
		return;
	}

	// Skip leading whitespace
	while (*sql && (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r')) {
		sql++;
	}

	// Case-insensitive comparison of first keyword
	if (strncasecmp(sql, "INSERT", 6) == 0) {
		extras->stmt_type = STMT_INSERT;
		// Extract table name: INSERT INTO table_name
		const char* into = FindCaseInsensitive(sql, "INTO");
		if (into) {
			into += 4;
			while (*into && (*into == ' ' || *into == '\t')) into++;
			const char* end = into;
			while (*end && *end != ' ' && *end != '\t' && *end != '(' && *end != '\n') end++;
			extras->table_name = std::string(into, end - into);
		}
	} else if (strncasecmp(sql, "UPDATE", 6) == 0) {
		extras->stmt_type = STMT_UPDATE;
		// Extract table name: UPDATE table_name SET
		sql += 6;
		while (*sql && (*sql == ' ' || *sql == '\t')) sql++;
		const char* end = sql;
		while (*end && *end != ' ' && *end != '\t' && *end != '\n') end++;
		extras->table_name = std::string(sql, end - sql);
	} else if (strncasecmp(sql, "DELETE", 6) == 0) {
		extras->stmt_type = STMT_DELETE;
		// Extract table name: DELETE FROM table_name
		const char* from = FindCaseInsensitive(sql, "FROM");
		if (from) {
			from += 4;
			while (*from && (*from == ' ' || *from == '\t')) from++;
			const char* end = from;
			while (*end && *end != ' ' && *end != '\t' && *end != '\n' && *end != ';') end++;
			extras->table_name = std::string(from, end - from);
		}
	} else if (strncasecmp(sql, "SELECT", 6) == 0) {
		extras->stmt_type = STMT_SELECT;
	} else {
		extras->stmt_type = STMT_OTHER;
	}

	// Note: table_id lookup is deferred to first NotifyCdc call for better performance
}

// Notify Noria of changes after write operations
void Statement::NotifyCdc() {
	Noria* noria = db->GetNoria();
	// Ultra-fast path: skip if no views registered at all (common case)
	if (!noria || !noria->HasAnyViews()) return;

	// Use session-based CDC if available - it will extract changes from
	// the session changeset which captures all modifications automatically
	noria->NotifyChange();

#if !USE_SESSION_CDC
	// Fallback: manual table-based invalidation when session extension not available
	// Check if this table has dependent views and get the table ID
	// table_id states: -1 = not checked, -2 = no views, >= 0 = has views
	if (extras->table_id == -1) {
		// First time - lookup table ID (this caches it for future calls)
		extras->table_id = noria->GetTableId(extras->table_name.c_str());
		if (extras->table_id < 0) {
			extras->table_id = -2;  // Mark as "no views"
			return;
		}
	} else if (extras->table_id == -2) {
		// Already checked, no views depend on this table
		return;
	}

	// Fast path: use table ID instead of string (no allocation in FFI)
	noria->QueueInvalidateById(extras->table_id);
#endif
}
