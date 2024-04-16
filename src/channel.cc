/*
 * Copyright (c) 2019-2021 Pavel Shramov <shramov@mexmat.net>
 *
 * tll is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include <sqlite3.h>

#include <variant>

#include "tll/channel/base.h"
#include "tll/channel/module.h"
#include "tll/scheme/util.h"
#include "tll/util/listiter.h"
#include "tll/util/memoryview.h"
#include "tll/util/string.h"

#include "common.h"
#include "sqlite-scheme.h"

using namespace tll;

class SQLite : public SQLBase<SQLite>
{
	std::map<int, std::pair<const tll::scheme::Message *, query_ptr_t>> _messages;

	bool _replace = false;

	query_ptr_t _select_statement = nullptr;
	const tll::scheme::Message * _select_message = nullptr;

 public:
	static constexpr std::string_view sqlite_control_scheme() { return sqlite_scheme::scheme; }

	static constexpr std::string_view channel_protocol() { return "sqlite"; }

	int _init(const tll::Channel::Url &, tll::Channel *master);
	int _open(const tll::ConstConfig &);
	int _close();
	void _destroy();

	int _post(const tll_msg_t *msg, int flags);
	int _process(long timeout, int flags);

 private:
	int _create_table(std::string_view table, const tll::scheme::Message *);
	int _create_statement(std::string_view table, const tll::scheme::Message *);
	int _create_index(const std::string_view &name, std::string_view key, bool unique);
	int _create_select_statement(std::string_view _table);
};

int SQLite::_init(const Channel::Url &url, Channel * master)
{
	if ((internal.caps & (caps::Input | caps::Output)) == caps::Input)
		return _log.fail(EINVAL, "SQLite channel is write-only");

	auto reader = channel_props_reader(url);

	_replace = reader.getT("replace", false);
	if (!reader)
		return _log.fail(EINVAL, "Invalid url: {}", reader.error());

	return SQLBase<SQLite>::_init(url, master);
}

int SQLite::_open(const ConstConfig &s)
{
	if (auto r = SQLBase<SQLite>::_open(s); r)
		return _log.fail(r, "Failed to open SQLite database");

	auto table_name = s.get("table");

	for (auto & m : tll::util::list_wrap(_scheme->messages)) {
		if (m.msgid == 0) {
			_log.debug("Message {} has no msgid, skip table check", m.name);
			continue;
		}

		auto table = tll::getter::get(m.options, "sql.table").value_or(std::string_view(m.name));

		if (_create_table(table, &m))
			return _log.fail(EINVAL, "Failed to create table '{}' for '{}'", table, m.name);
		if (_create_statement(table, &m))
			return _log.fail(EINVAL, "Failed to prepare SQL statement for '{}'", m.name);
	}

	if (table_name && table_name->size()) {
		std::string_view tname = *table_name;
		for (auto& m : _messages) {
			const auto& options = m.second.first->options;
			const auto& name = m.second.first->name;
			if (tname == tll::getter::get(options, "sql.table").value_or(std::string_view(name))) {
				_select_message = m.second.first;
			}
		}
		if (_create_select_statement(tname)) {
			return EINVAL;
		}
		_update_dcaps(dcaps::Process | dcaps::Pending);
	}

	return 0;
}

namespace {

template <typename Iter>
std::string join(std::string_view sep, const Iter &begin, const Iter &end)
{
	std::string r;
	bool comma = false;
	for (Iter it = begin; it != end; ++it) {
		if (comma)
			r += sep;
		comma = true;
		r += tll::conv::to_string(*it);
	}
	return r;
}

template <typename Iter>
std::string join(const Iter &begin, const Iter &end)
{
	return join(", ", begin, end);
}

tll::result_t<std::string> sql_type(const tll::scheme::Field *field)
{
	using tll::scheme::Field;
	switch (field->type) {
	case Field::Int8:
	case Field::UInt8:
	case Field::Int16:
	case Field::UInt16:
	case Field::Int32:
	case Field::UInt32:
	case Field::Int64:
		return "INTEGER";
	case Field::UInt64:
		return tll::error("UInt64 not supported");

	case Field::Double:
		return "REAL";

	case Field::Decimal128:
		return tll::error("Decimal128 not supported yet");
		//return "REAL";

	case Field::Bytes:
		if (field->sub_type == Field::ByteString)
			return "VARCHAR";
		return "REAL";

	case Field::Message:
		return tll::error("Nested messages not supported");
	case Field::Array:
		return tll::error("Nested arrays not supported");
	case Field::Pointer:
		if (field->type_ptr->type == Field::Int8 && field->sub_type == Field::ByteString) {
			return "VARCHAR";
		}
		return tll::error("Nested arrays not supported");
	case Field::Union:
		return tll::error("Union not supported");
	}
	return tll::error("Invalid field type");
}

template <typename Buf>
int sql_bind(sqlite3_stmt * sql, int idx, const tll::scheme::Field *field, const Buf &data)
{
	using tll::scheme::Field;
	switch (field->type) {
	case Field::Int8:
	case Field::Int16:
	case Field::Int32:
	case Field::Int64:
	case Field::UInt8:
	case Field::UInt16:
	case Field::UInt32:
		return sqlite3_bind_int64(sql, idx, tll::scheme::read_size(field, data));

	case Field::Double:
		return sqlite3_bind_double(sql, idx, *data.template dataT<double>());

	case Field::UInt64:
	case Field::Decimal128:
		return SQLITE_ERROR;

	case Field::Bytes:
		if (field->sub_type == Field::ByteString) {
			auto str = data.template dataT<char>();
			return sqlite3_bind_text(sql, idx, str, strnlen(str, field->size), SQLITE_STATIC);
		}

		return sqlite3_bind_blob(sql, idx, data.data(), field->size, SQLITE_STATIC);

	case Field::Message:
		return SQLITE_ERROR;
	case Field::Array:
		return SQLITE_ERROR;
	case Field::Pointer:
		if (field->type_ptr->type == Field::Int8 && field->sub_type == Field::ByteString) {
			auto ptr = tll::scheme::read_pointer(field, data);
			if (!ptr)
				return SQLITE_ERROR;
			if (ptr->size == 0)
				return sqlite3_bind_text(sql, idx, "", 0, SQLITE_STATIC);
			return sqlite3_bind_text(sql, idx, data.view(ptr->offset).template dataT<char>(), ptr->size - 1, SQLITE_STATIC);
		}
		return SQLITE_ERROR;
	case Field::Union:
		return SQLITE_ERROR;
	}
	return SQLITE_ERROR;
}

template <typename Buf>
int sql_column_(sqlite3_stmt * sql, int idx, const tll::scheme::Field *field, Buf data)
{
	using tll::scheme::Field;
	switch (field->type) {
	case Field::Int8:
		*data.template dataT<int8_t>() = sqlite3_column_int64(sql, idx);
		return 0;
	case Field::Int16:
		*data.template dataT<int16_t>() = sqlite3_column_int64(sql, idx);
		return 0;
	case Field::Int32:
		*data.template dataT<int32_t>() = sqlite3_column_int64(sql, idx);
		return 0;
	case Field::Int64:
		*data.template dataT<int64_t>() = sqlite3_column_int64(sql, idx);
		return 0;
	case Field::UInt8:
		*data.template dataT<uint8_t>() = sqlite3_column_int64(sql, idx);
		return 0;
	case Field::UInt16:
		*data.template dataT<uint16_t>() = sqlite3_column_int64(sql, idx);
		return 0;
	case Field::UInt32:
		*data.template dataT<uint32_t>() = sqlite3_column_int64(sql, idx);
		return 0;
	case Field::Double:
		*data.template dataT<double>() = sqlite3_column_double(sql, idx);
		return 0;
	case Field::Decimal128:
		return SQLITE_ERROR;

	case Field::Bytes: {
		if (field->sub_type == Field::ByteString) {
			auto string = (const char*) sqlite3_column_text(sql, idx);
			memcpy(data.template dataT<char>(), string, std::min(strlen(string), field->size));
			return 0;
		}
		auto b_string = (sqlite3_column_blob(sql, idx));
		memcpy(data.data(), b_string, field->size);
		return 0;
	}
	case Field::Pointer: {
		if (field->type_ptr->type == Field::Int8 && field->sub_type == Field::ByteString) {
			auto string = std::string_view((const char*) sqlite3_column_text(sql, idx));
			tll::scheme::generic_offset_ptr_t ptr;
			ptr.size = string.size() + 1;
			ptr.offset = data.size();
			ptr.entity = 1;
			data.resize(data.size() + string.size() + 1);
			write_pointer(field, data, ptr);
			memcpy(data.view(ptr.offset).template dataT<char>(), string.data(), string.size() + 1);
			return 0;
		}
		return SQLITE_ERROR;
	}
	default:
		return SQLITE_ERROR;
	}

	return SQLITE_ERROR;
}

}

int SQLite::_create_table(std::string_view table, const tll::scheme::Message * msg)
{
	query_ptr_t sql;

	sql.reset(_prepare("SELECT name FROM sqlite_master WHERE name=?"));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare check statement");

	sqlite3_bind_text(sql.get(), 1, table.data(), table.size(), SQLITE_STATIC);
	auto r = sqlite3_step(sql.get());
	if (r == SQLITE_ROW) {
		_log.debug("Table '{}' exists", table);
		return 0;
	} else if (r != SQLITE_DONE)
		return _log.fail(EINVAL, "Failed to check table '{}'", table);

	_log.info("Create table '{}'", table);
	std::list<std::string> fields;

	fields.push_back("`_tll_seq` INTEGER");
	for (auto & f : tll::util::list_wrap(msg->fields)) {
		auto t = sql_type(&f);
		if (!t)
			return _log.fail(EINVAL, "Message {} field {}: {}", msg->name, f.name, t.error());
		fields.push_back(fmt::format("`{}` {} NOT NULL", f.name, *t));

		auto pkey = tll::getter::getT(f.options, "sql.primary-key", false);
		if (f.type == f.Pointer)
			pkey = tll::getter::getT(f.type_ptr->options, "sql.primary-key", false);

		if (!pkey)
			_log.warning("Invalid primary-key option: {}", pkey.error());
		else if (*pkey) {
			_log.debug("Field {} is primary key", f.name);
			fields.back() += " PRIMARY KEY";
		}
	}

	sql.reset(_prepare(fmt::format("CREATE TABLE `{}` ({})", table, join(fields.begin(), fields.end()))));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare CREATE statement");

	if (sqlite3_step(sql.get()) != SQLITE_DONE)
		return _log.fail(EINVAL, "Failed to create table '{}'", table);

	{
		auto index = tll::getter::getT(msg->options, "sql.index", _seq_index, {{"no", Index::No}, {"yes", Index::Yes}, {"unique", Index::Unique}});
		if (!index) {
			_log.warning("Invalid sql.index option for {}: {}", msg->name, index.error());
		} else if (*index != Index::No) {
			if (_create_index(table, "_tll_seq", *index == Index::Unique))
				return _log.fail(EINVAL, "Failed to create seq index for table {}", table);
		}
	}

	for (auto & f : tll::util::list_wrap(msg->fields)) {
		auto index = tll::getter::getT(f.options, "sql.index", Index::No, {{"no", Index::No}, {"yes", Index::Yes}, {"unique", Index::Unique}});
		if (!index) {
			_log.warning("Invalid sql.index option for {}.{}: {}", msg->name, f.name, index.error());
		} else if (*index != Index::No) {
			if (_create_index(table, f.name, *index == Index::Unique))
				return _log.fail(EINVAL, "Failed to create index {} for table {}", f.name, table);
		}
	}

	return 0;
}

int SQLite::_create_select_statement(std::string_view table) {
	std::list<std::string> names;
	names.push_back("`_tll_seq`");
	for (auto & f : tll::util::list_wrap(_select_message->fields)) {
		names.push_back(fmt::format("`{}`", f.name));
	}
	std::string select = fmt::format("SELECT {} FROM `{}`", join(names.begin(), names.end()), table);
	_select_statement.reset(_prepare(select));
	if (!_select_statement) {
		return _log.fail(EINVAL, "Failed to prepare select statement for table {}: {}", table, select);
	}
	return 0;
}

int SQLite::_create_statement(std::string_view table, const tll::scheme::Message *msg)
{
	std::list<std::string> names;
	names.push_back("`_tll_seq`");
	for (auto & f : tll::util::list_wrap(msg->fields))
		names.push_back(fmt::format("`{}`", f.name));

	std::string_view operation = "INSERT";
	if (_replace)
		operation = "REPLACE";
	auto insert = fmt::format("{} INTO `{}`({}) VALUES ", operation, table, join(names.begin(), names.end()));
	for (auto & i : names)
		i = "?";
	insert += fmt::format("({})", join(names.begin(), names.end()));

	query_ptr_t sql;

	sql.reset(_prepare(insert));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare update statement for table {}: {}", table, insert);

	_messages.emplace(msg->msgid, std::make_pair(msg, std::move(sql)));

	return 0;
}

int SQLite::_create_index(const std::string_view &name, std::string_view key, bool unique)
{
	_log.debug("Create index for {}: key {}", name, key);
	query_ptr_t sql;

	std::string_view ustr = unique ? "UNIQUE " : "";
	auto str = fmt::format("CREATE {} INDEX `_tll_{}_{}` on `{}`(`{}`)", ustr, name, key, name, key);
	sql.reset(_prepare(str));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare index statement: {}", str);

	if (sqlite3_step(sql.get()) != SQLITE_DONE)
		return _log.fail(EINVAL, "Failed to create index for '{}': {}", name, key);
	return 0;
}

int SQLite::_close()
{
	_messages.clear();
	return SQLBase<SQLite>::_close();
}

int SQLite::_post(const tll_msg_t *msg, int flags)
{
	if (msg->type != TLL_MESSAGE_DATA && msg->type != TLL_MESSAGE_CONTROL)
		return 0;

	if (msg->type == TLL_MESSAGE_CONTROL) {
		if (msg->msgid == sqlite_scheme::TableName::id) {
			_select_message = _messages.at(((const sqlite_scheme::TableName*) msg->data)->msgid).first;
			if (_create_select_statement(std::string_view(_select_message->name))) {
				return EINVAL;
			}
			_update_dcaps(dcaps::Process | dcaps::Pending);
			return 0;
		}
		return EINVAL;
	}

	if (msg->msgid == 0)
		return _log.fail(EINVAL, "Unable to insert message without msgid");
	auto it = _messages.find(msg->msgid);
	if (it == _messages.end())
		return _log.fail(ENOENT, "Message {} not found", msg->msgid);
	auto & [message, insert] = it->second;

	if (!_bulk_counter) {
		sqlite3_exec(_db.get(), "BEGIN", 0, 0, 0);
	}

	sqlite3_reset(insert.get());

	auto view = tll::make_view(*msg);

	int idx = 1;
	sqlite3_bind_int64(insert.get(), idx++, (sqlite3_int64) msg->seq);
	for (auto & f : tll::util::list_wrap(message->fields)) {
		if (sql_bind(insert.get(), idx++, &f, view.view(f.offset)))
			return _log.fail(EINVAL, "Failed to bind field {}", f.name);
	}
	auto r = sqlite3_step(insert.get());
	if (r != SQLITE_DONE)
		return _log.fail(EINVAL, "Failed to insert data: {}", sqlite3_errmsg(_db.get()));
	if (++_bulk_counter >= _bulk_size) {
		_commit();
	}
	return 0;
}

int SQLite::_process(long timeout, int flags) {
	int result = sqlite3_step(_select_statement.get()); // 100 AS SQLITE_ROW, 101 AS SQLITE_DONE

	if (result == SQLITE_ROW) {
		tll_msg_t msg = {TLL_MESSAGE_DATA, _select_message->msgid,
			sqlite3_column_int64(_select_statement.get(), 0)};
		std::vector<unsigned char> buf;
		buf.resize(_select_message->size);
		auto view = tll::make_view(buf);
		int idx = 1;
		for (auto & f : tll::util::list_wrap(_select_message->fields)) {
			if (sql_column_(_select_statement.get(), idx++, &f, view.view(f.offset))) {
				return EINVAL;
			}
		}
		msg.size = buf.size();
		msg.data = buf.data();
		_callback_data(&msg);
		return 0;
	} else if (result == SQLITE_DONE) {
		_update_dcaps(0, dcaps::Process | dcaps::Pending);
		tll_msg_t msg = {TLL_MESSAGE_CONTROL, sqlite_scheme::EndOfData::id};
		_callback(&msg);
		SQLite::_close();
		return 0;
	}
	_log.debug("nouret: {}\t", sqlite3_column_int(_select_statement.get(), 0));
	return 0;
}

TLL_DEFINE_IMPL(SQLite);

TLL_DEFINE_MODULE(SQLite);
