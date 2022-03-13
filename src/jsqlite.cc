/*
 * Copyright (c) 2019-2022 Pavel Shramov <shramov@mexmat.net>
 *
 * tll is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include <sqlite3.h>

#include <variant>

#include <tll/channel/base.h>
#include <tll/channel/module.h>
#include <tll/util/json.h>
#include <tll/util/refptr.h>
#include <tll/util/string.h>

#include "common.h"

using namespace tll;

class JSQLite : public tll::channel::Base<JSQLite>
{
 	tll::json::JSON _json = { _log };

	std::shared_ptr<sqlite3> _db = { nullptr, sqlite3_close };
	query_ptr_t _select;
	query_ptr_t _insert;

	JSQLite * master = nullptr;
	std::string _path;
	std::string _table;
	std::vector<std::variant<std::string, long long, double>> _query;
	bool _autoclose;

	bool _transaction = false;
	size_t _bulk_size = 10000;
	size_t _bulk_counter = 0;

	tll::json::JSON * json() { if (master) return &master->_json; return &_json; }

 public:
	static constexpr std::string_view channel_protocol() { return "jsqlite"; }

	int _init(const tll::Channel::Url &, tll::Channel *master);
	int _open(const tll::ConstConfig &);
	int _close();
	void _destroy();

	int _post(const tll_msg_t *msg, int flags);
	int _process(long timeout, int flags);

 private:
	int _create_table();
	int _create_index(const std::string_view &name, const std::string_view &key);

	sqlite3_stmt * _prepare(const std::string_view query)
	{
		_log.debug("Prepare SQL statement:\n\t{}", query);
		sqlite3_stmt * sql = nullptr;
		const char * tail = nullptr;
		auto r = sqlite3_prepare_v2(_db.get(), query.data(), query.size(), &sql, &tail);
		if (r != SQLITE_OK)
			return _log.fail(nullptr, "Failed to prepare statement: {}\n\t{}", sqlite3_errmsg(_db.get()), query);
		return sql;
	}
};

int JSQLite::_init(const Channel::Url &url, Channel * master)
{
	if (master) {
		this->master = channel_cast<JSQLite>(master);
		if (!this->master)
			return _log.fail(EINVAL, "Parent {} must be jsqlite:// channel", master->name());
		_table = this->master->_table;
		return 0;
	}

	if (!_scheme_url)
		return _log.fail(EINVAL, "Channel needs scheme");

	if (!url.host().size())
		return _log.fail(EINVAL, "No path to database");
	_path = url.host();

	auto reader = channel_props_reader(url);
	//if (_json.init(reader))
	//	return _log.fail(EINVAL, "Failed to init JSON encoder");

	_table = reader.getT<std::string>("table");
	_bulk_size = reader.getT("bulk-size", _bulk_size);
	if ((internal.caps & (caps::Input | caps::Output)) == caps::Input)
		_autoclose = reader.getT("autoclose", false);
	if (!reader)
		return _log.fail(EINVAL, "Invalid url: {}", reader.error());

	return 0;
}

int JSQLite::_open(const ConstConfig &s)
{
	_bulk_counter = 0;

	if (master) {
		_db = master->_db;
		auto s = master->self()->scheme();
		if (!s)
			return _log.fail(EINVAL, "Parent {} without scheme", master->self()->name());
		_scheme.reset(s->copy());
		return 0;
	}

	if (_json.init_scheme(_scheme.get()))
		return _log.fail(EINVAL, "Failed to initialize scheme");

	sqlite3 * db = nullptr;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	if ((internal.caps & TLL_CAPS_INOUT) == TLL_CAPS_INPUT)
		flags = SQLITE_OPEN_READONLY;
	auto r = sqlite3_open_v2(_path.c_str(), &db, flags, nullptr);
	if (r)
		return _log.fail(EINVAL, "Failed to open '{}': {}", _path, sqlite3_errstr(r));
	_db.reset(db, sqlite3_close);

	if (internal.caps & caps::Output) {
		if (_create_table())
			return _log.fail(EINVAL, "Failed to create table '{}'", _table);

		auto str = fmt::format("REPLACE INTO `{}`(`seq`, `name`, `data`) VALUES (?, ?, ?)", _table);
		_insert.reset(_prepare(str));
		if (!_insert)
			return _log.fail(EINVAL, "Failed to prepare REPLACE statement");
	}

	if (internal.caps & caps::Input) {
		auto str = fmt::format("SELECT `seq`, `name`, `data` FROM `{}`", _table);
		auto name = s.get("query");
		_query.clear();
		if (name) {
			str += " WHERE `name`= ?";
			_query.push_back(std::string(*name));
			auto msg = json()->lookup(*name);
			if (!msg)
				return _log.fail(ENOENT, "Query for message not in scheme: '{}'", *name);
		}

		for (auto & [k,cfg] : s.browse("query.**")) {
			if (!_query.size())
				_log.warning("Query without message name, string comparison");
			auto value = cfg.get();
			if (!value)
				continue;
			std::string_view v = *value;
			_log.debug("Query: {}={}", k, v);
			std::string_view sep = _query.size()?"AND":"WHERE";
			auto key = k.substr(strlen("query."));
			str += fmt::format(" {} json_extract(data, \"$.{}\") = ?", sep, key);
			if (!name) {
				_query.push_back(std::string(v));
				continue;
			}
			auto msg = json()->lookup(*name);
			using namespace tll::scheme;
			const Field * field = nullptr;

			for (auto i : tll::split<'.'>(key)) {
				if (field && field->type != Field::Message)
					return _log.fail(EINVAL, "Invalid key '{}': message '{}' field '{}' is not submessage", key, msg->name, i);
				auto m = field?field->type_msg:msg;
				auto meta = static_cast<const json::message_meta_t *>(m->user);
				if (!meta)
					return _log.fail(EINVAL, "Message without metadata: {}", m->name);
				auto it = meta->index.find(i);
				if (it == meta->index.end())
					return _log.fail(EINVAL, "Invalid key '{}': message '{}' has no field '{}'", key, m->name, i);
				field = it->second;
			}

			switch(field->type) {
			case Field::Int8:
			case Field::Int16:
			case Field::Int32:
			case Field::Int64: {
				auto r = conv::to_any<long long>(v);
				if (!r)
					return _log.fail(EINVAL, "Invalid integer value for key '{}' '{}': {}", key, v, r.error());
				_query.push_back(*r);
				break;
			}
			case Field::Double: {
				auto r = conv::to_any<double>(v);
				if (!r)
					return _log.fail(EINVAL, "Invalid double value for key '{}' '{}': {}", key, v, r.error());
				_query.push_back(*r);
				break;
			}
			default:
				_query.push_back(std::string(v));
				break;
			}
		}

		_select.reset(_prepare(str));
		if (!_select)
			return _log.fail(EINVAL, "Failed to prepare SELECT statement");
		for (auto i = 0u; i < _query.size(); i++) {
			auto & v = _query[i];
			if (std::holds_alternative<std::string>(v))
				sqlite3_bind_text(_select.get(), 1 + i, std::get<std::string>(v).data(), std::get<std::string>(v).size(), SQLITE_STATIC);
			else if (std::holds_alternative<long long>(v))
				sqlite3_bind_int64(_select.get(), 1 + i, std::get<long long>(v));
			else if (std::holds_alternative<double>(v))
				sqlite3_bind_double(_select.get(), 1 + i, std::get<double>(v));
		}
		_update_dcaps(dcaps::Pending);
	}

	return 0;
}

int JSQLite::_create_table()
{
	query_ptr_t sql;

	sql.reset(_prepare("SELECT name FROM sqlite_master WHERE name=?"));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare check statement");

	sqlite3_bind_text(sql.get(), 1, _table.data(), _table.size(), SQLITE_STATIC);
	auto r = sqlite3_step(sql.get());
	if (r == SQLITE_ROW) {
		_log.debug("Table '{}' exists", _table);
		return 0;
	} else if (r != SQLITE_DONE)
		return _log.fail(EINVAL, "Failed to check table '{}'", _table);

	_log.info("Create table '{}'", _table);
	sql.reset(_prepare(fmt::format("CREATE TABLE `{}` (`seq` INTEGER, `name` VARCHAR NOT NULL, `data` TEXT)", _table)));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare CREATE statement");

	if (sqlite3_step(sql.get()) != SQLITE_DONE)
		return _log.fail(EINVAL, "Failed to create table '{}'", _table);

	for (auto & m : tll::util::list_wrap(_scheme->messages)) {
		auto key = scheme::options_map(m.options).get("key");
		if (!key) continue;
		_create_index(m.name, *key);
	}

	return 0;
}

int JSQLite::_create_index(const std::string_view &name, const std::string_view &key)
{
	_log.debug("Create index for {}: key {}", name, key);
	query_ptr_t sql;

	auto str = fmt::format("CREATE UNIQUE INDEX `json_{}_{}` on `{}`(json_extract(data, \"$.{}\")) WHERE `name`='{}'", _table, name, _table, key, name);
	sql.reset(_prepare(str));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare index statement");

	if (sqlite3_step(sql.get()) != SQLITE_DONE)
		return _log.fail(EINVAL, "Failed to create index for '{}': {}", name, key);
	return 0;
}

int JSQLite::_close()
{
	if (_bulk_counter) {
		sqlite3_exec(_db.get(), "COMMIT", 0, 0, 0);
		_bulk_counter = 0;
	}
	_select.reset();
	_insert.reset();
	_db.reset();
	return 0;
}

int JSQLite::_post(const tll_msg_t *msg, int flags)
{
	if (msg->type != TLL_MESSAGE_DATA)
		return 0;
	if (!_insert)
		return _log.fail(ENOSYS, "Inserts not enabled");
	auto message = json()->lookup(msg->msgid);
	if (!message)
		return _log.fail(EINVAL, "Unknown message {}", msg->msgid);

	tll_msg_t jmsg = {};
	auto jdata = json()->encode(message, msg, &jmsg);
	if (!jdata) {
		tll_channel_log_msg(self(), _log.name(), logger::Warning, tll::channel::log_msg_format::Scheme, msg, "Failed message", strlen("Failed message"));
		return state_fail(EINVAL, "Failed to encode JSON data");
	}
	
	if (!_bulk_counter) {
		sqlite3_exec(_db.get(), "BEGIN", 0, 0, 0);
	}
	sqlite3_reset(_insert.get());
	sqlite3_bind_int64(_insert.get(), 1, (sqlite3_int64) msg->seq);
	sqlite3_bind_text(_insert.get(), 2, message->name, -1, SQLITE_STATIC);
	sqlite3_bind_text(_insert.get(), 3, (const char *) jdata->data, jdata->size, SQLITE_STATIC);
	auto r = sqlite3_step(_insert.get());
	if (r != SQLITE_DONE)
		return _log.fail(EINVAL, "Failed to insert data");
	if (++_bulk_counter >= _bulk_size) {
		sqlite3_exec(_db.get(), "COMMIT", 0, 0, 0);
		_bulk_counter = 0;
	}
	return 0;
}

int JSQLite::_process(long timeout, int flags)
{
	if (!_select)
		return EAGAIN;
	auto r = sqlite3_step(_select.get());
	if (r == SQLITE_DONE) {
		_log.debug("Query finished");
		_select.reset();
		if (_autoclose) {
			_log.debug("Autoclosing channel");
			close();
		}
		return EAGAIN;
	} else if (r != SQLITE_ROW)
		return _log.fail(EINVAL, "Failed to select data");


	tll_msg_t jmsg = {};
	jmsg.seq = sqlite3_column_int64(_select.get(), 0);

	auto nsize = sqlite3_column_bytes(_select.get(), 1);
	if (!nsize)
		return _log.fail(EINVAL, "NULL name for message (seq {})", jmsg.seq);
	std::string_view name((const char *) sqlite3_column_text(_select.get(), 1), nsize);

	auto message = json()->lookup(name);
	if (!message)
		return _log.fail(EINVAL, "Unknown message {}", name);

	auto size = sqlite3_column_bytes(_select.get(), 2);
	if (size == 0) {
		_callback_data(&jmsg);
		return 0;
	}
	jmsg.size = size;
	jmsg.data = sqlite3_column_blob(_select.get(), 2);

	auto data = json()->decode(message, &jmsg, &jmsg);
	if (!data)
		return _log.fail(EINVAL, "Failed to decode JSON for message {} (seq {})", message->name, jmsg.seq);

	jmsg.size = data->size;
	jmsg.data = data->data;
	_callback_data(&jmsg);
	return 0;
}

TLL_DEFINE_IMPL(JSQLite);

TLL_DEFINE_MODULE(JSQLite);
