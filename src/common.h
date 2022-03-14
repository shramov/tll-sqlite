#ifndef _COMMON_H
#define _COMMON_H

#include <sqlite3.h>

#include <memory>

// clang-format off
struct sqlite3_delete { void operator ()(sqlite3 *ptr) const { sqlite3_close(ptr); } };
struct sqlite3_stmt_delete { void operator ()(sqlite3_stmt *ptr) const { sqlite3_finalize(ptr); } };
// clang-format on

using query_ptr_t = std::unique_ptr<sqlite3_stmt, sqlite3_stmt_delete>;

template <typename T>
class SQLBase : public tll::channel::Base<T>
{
 protected:
	std::shared_ptr<sqlite3> _db = { nullptr, sqlite3_close };

	std::string _path;

	bool _replace = false;
	enum class Index { No, Yes, Unique } _seq_index = Index::Unique;
	enum class Journal { Default, Wal } _journal = Journal::Wal;

	size_t _bulk_size = 0;
	size_t _bulk_counter = 0;

	static constexpr std::string_view sqlite_control_scheme();

 public:
	static constexpr auto process_policy() { return tll::channel::Base<T>::ProcessPolicy::Custom; }

	int _init(const tll::Channel::Url &, tll::Channel *master);
	int _open(const tll::ConstConfig &);
	int _close();

 protected:
	sqlite3_stmt * _prepare(const std::string_view query)
	{
		this->_log.debug("Prepare SQL statement:\n\t{}", query);
		sqlite3_stmt * sql = nullptr;
		const char * tail = nullptr;
		auto r = sqlite3_prepare_v2(_db.get(), query.data(), query.size(), &sql, &tail);
		if (r != SQLITE_OK)
			return this->_log.fail(nullptr, "Failed to prepare statement: {}\n\t{}", sqlite3_errmsg(_db.get()), query);
		return sql;
	}

	int _commit()
	{
		this->_log.debug("Commit transaction");
		if (sqlite3_exec(_db.get(), "COMMIT", 0, 0, 0))
			return this->_log.fail(EINVAL, "Failed to commit pending transaction: {}", sqlite3_errmsg(_db.get()));
		_bulk_counter = 0;
		return 0;
	}
};

template <typename T>
int SQLBase<T>::_init(const tll::Channel::Url &url, tll::Channel * master)
{
	this->_scheme_control.reset(this->context().scheme_load(this->channelT()->sqlite_control_scheme()));
	if (!this->_scheme_control.get())
		return this->_log.fail(EINVAL, "Failed to load control scheme");

	if (!this->_scheme_url)
		return this->_log.fail(EINVAL, "Channel needs scheme");

	if (!url.host().size())
		return this->_log.fail(EINVAL, "No path to database");
	_path = url.host();

	auto reader = this->channel_props_reader(url);

	_replace = reader.getT("replace", false);
	_seq_index = reader.getT("seq-index", Index::Unique, {{"no", Index::No}, {"yes", Index::Yes}, {"unique", Index::Unique}});
	_journal = reader.getT("journal", Journal::Wal, {{"wal", Journal::Wal}, {"default", Journal::Default}});
	_bulk_size = reader.getT("bulk-size", 0u);
	if (!reader)
		return this->_log.fail(EINVAL, "Invalid url: {}", reader.error());

	return 0;
}

template <typename T>
int SQLBase<T>::_open(const tll::ConstConfig &s)
{
	_bulk_counter = 0;

	sqlite3 * db = nullptr;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	auto r = sqlite3_open_v2(_path.c_str(), &db, flags, nullptr);
	if (r)
		return this->_log.fail(EINVAL, "Failed to open '{}': {}", _path, sqlite3_errstr(r));
	_db.reset(db, sqlite3_close);

	if (_journal == Journal::Wal) {
		if (sqlite3_exec(_db.get(), "PRAGMA journal_mode=wal", 0, 0, 0))
			return this->_log.fail(EINVAL, "Failed to change journal_mode to WAL: {}", sqlite3_errmsg(_db.get()));
	}

	return 0;
}

template <typename T>
int SQLBase<T>::_close()
{
	if (_bulk_counter)
		_commit();
	_db.reset();
	return 0;
}

#endif//_COMMON_H
