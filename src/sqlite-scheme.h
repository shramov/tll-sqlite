#pragma once

#include <string_view>
#include "tll/scheme/types.h"

#pragma pack(push, 1)

namespace sqlite_scheme {

static constexpr std::string_view scheme = R"(yamls://
- name: data_end
  id: 1

- name: table_name
  id: 2
  fields:
    - {name: msgid, type: int64}
)";

struct data_end {
	static constexpr int id = 1;
};

struct table_name {
	static constexpr int id = 2;
	int64_t msgid;
};

}

#pragma pack(pop)
