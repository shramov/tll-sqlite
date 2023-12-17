#pragma once

#include <string_view>
#include "tll/scheme/types.h"

#pragma pack(push, 1)

namespace sqlite_scheme {

static constexpr std::string_view scheme = R"(yamls://
- name: EndOfData
  id: 1

- name: TableName
  id: 2
  fields:
    - {name: msgid, type: int64}
)";

struct EndOfData {
	static constexpr int id = 1;
};

struct TableName {
	static constexpr int id = 2;
	int64_t msgid;
};

}

#pragma pack(pop)
