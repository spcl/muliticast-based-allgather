# SPDX-FileCopyrightText: Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

sample_dev_srcs =  name + '_dev.c'
app_common_dir = 'apps/common'
app_out_name = dev_app_name + '.a'
dpacc_mode = '--application'

inc_app_host = run_command(tools , '--list-all',
			   join_paths(meson.current_source_dir(), '..', 'host', '*_com.hpp'),
			   check: true).stdout().strip().split('\n')
inc_tgt_app = inc_tgt_app_common + inc_app_host

asm_compile = ['-disable-asm-checks', '-disable-asm-checks-nowarn']

temp_app_target = custom_target('app_' + name,
				command: [ dpacc_build_script, dpacc_mode,
					'--app_name', dev_app_name,
					'--flexio_root', '@SOURCE_ROOT@',
					'--build_root', '@BUILD_ROOT@',
					'--srcs', '@INPUT@',
					'--dpacc_build_dir', '@OUTDIR@',
					'--external_cc_options', device_cc_params,
					'--additional_include_directories', join_paths(dir_base, app_common_dir),
					'--additional_ld_libs', 'app_common_dev',
					'--additional_lib_paths', join_paths(project_build_root, app_common_dir),
					'--additional_dpacc_options', dpa_cc_options + [asm_compile],
					'--debug', debug_dpacc ],
				output: app_out_name,
				input: sample_dev_srcs,
				depends: [tgt_flexio_dev, tgt_app_archive],
				depend_files: inc_tgt_app)

temp_dep = declare_dependency(link_args : join_paths(meson.current_build_dir(), app_out_name))
