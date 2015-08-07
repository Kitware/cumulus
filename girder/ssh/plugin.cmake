add_python_test(ssh PLUGIN ssh)
add_python_style_test(
  python_static_analysis_ssh
  "${PROJECT_SOURCE_DIR}/plugins/ssh/server"
)

