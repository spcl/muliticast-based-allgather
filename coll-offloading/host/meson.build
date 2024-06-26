#dpa_app = c.find_library(dev_app_name,
#                         dirs: dpacc_output,
#                         required: true)

src = files('ibv_transport.cpp',
            'dpa_transport.cpp'
)

cpp = meson.get_compiler('cpp')

executable(name,
        [name + '.cpp'],
        sources: src,
        native: true,
        include_directories: [top_inc, app_common_inc],
        dependencies: [ibverbs_dep, mlx5_dep, temp_dep],
        link_with: [libflexio, libapp_common_host],
        link_depends : temp_app_target
)