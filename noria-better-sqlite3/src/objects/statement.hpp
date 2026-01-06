class Statement : public node::ObjectWrap { friend class StatementIterator;
public:

	~Statement();

	// Whenever this is used, db->RemoveStatement must be invoked beforehand.
	void CloseHandles();

	// Used to support ordered containers.
	static inline bool Compare(Statement const * const a, Statement const * const b) {
		return a->extras->id < b->extras->id;
	}

	// Returns the Statement's bind map (creates it upon first execution).
	BindMap* GetBindMap(v8::Isolate* isolate);

	static INIT(Init);

	// Statement types for CDC
	enum StatementType {
		STMT_SELECT = 0,
		STMT_INSERT = 1,
		STMT_UPDATE = 2,
		STMT_DELETE = 3,
		STMT_OTHER = 4
	};

private:

	// A class for holding values that are less often used.
	class Extras { friend class Statement;
		explicit Extras(sqlite3_uint64 id);
		BindMap bind_map;
		const sqlite3_uint64 id;
		int noria_view_id;  // -1 if not registered, >= 0 if registered
		std::vector<v8::Global<v8::Name>> column_names;  // Cached column names for fast row construction
		StatementType stmt_type;  // Type of statement for CDC
		std::string table_name;   // Affected table for INSERT/UPDATE/DELETE
		int table_id;             // Cached table ID for fast CDC (-1 = not checked, -2 = no views)
	};

	explicit Statement(
		Database* db,
		sqlite3_stmt* handle,
		sqlite3_uint64 id,
		bool returns_data
	);

	// Try to register this statement as a Noria view if it's a SELECT
	void TryRegisterNoriaView(v8::Isolate* isolate);

	// Build column names cache for Noria row construction
	void CacheColumnNames(v8::Isolate* isolate);

	// Attempt to serve get() from Noria cache
	bool TryNoriaGet(v8::Isolate* isolate, const v8::FunctionCallbackInfo<v8::Value>& info, bool consistent_read);

	// Attempt to serve all() from Noria cache
	bool TryNoriaAll(v8::Isolate* isolate, const v8::FunctionCallbackInfo<v8::Value>& info, bool consistent_read);

	// Detect statement type and extract table name
	void DetectStatementType();

	// Notify Noria of changes after write operations
	void NotifyCdc();

	static NODE_METHOD(JS_new);
	static NODE_METHOD(JS_run);
	static NODE_METHOD(JS_get);
	static NODE_METHOD(JS_all);
	static NODE_METHOD(JS_iterate);
	static NODE_METHOD(JS_bind);
	static NODE_METHOD(JS_pluck);
	static NODE_METHOD(JS_expand);
	static NODE_METHOD(JS_raw);
	static NODE_METHOD(JS_safeIntegers);
	static NODE_METHOD(JS_columns);
	static NODE_GETTER(JS_busy);

	Database* const db;
	sqlite3_stmt* const handle;
	Extras* const extras;
	bool alive;
	bool locked;
	bool bound;
	bool has_bind_map;
	bool safe_ints;
	char mode;
	const bool returns_data;
};
