project('tll-sqlite', 'cpp', version: '0.0.0', default_options: ['cpp_std=c++17', 'werror=true', 'optimization=2'])

include = [include_directories('src')]

tll = dependency('tll')
fmt = dependency('fmt')
sqlite = dependency('sqlite3')

lib = shared_library('tll-sqlite',
	['src/channel.cc'],
	include_directories : include,
	dependencies : [fmt, sqlite, tll],
	install : true,
)

lib = shared_library('tll-jsqlite',
	['src/jsqlite.cc'],
	include_directories : include,
	dependencies : [fmt, sqlite, tll],
	install : true,
)

test('pytest', import('python').find_installation('python3')
	, args: ['-m', 'pytest', '-v', '--log-level=DEBUG', 'tests/']
	, env: 'BUILD_DIR=@0@'.format(meson.current_build_dir())
	, workdir: meson.current_source_dir()
)
