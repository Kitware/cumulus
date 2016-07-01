# Set this variable to the minimum coverage percentage to be considered passing
#set(COVERAGE_MINIMUM_PASS 90)
#set(JS_COVERAGE_MINIMUM_PASS 75)

python_tests_init()

add_python_style_test(python_static_analysis_cumulus "${PROJECT_SOURCE_DIR}/cumulus")
#add_python_style_test(flake8_cumulus_tests "${PROJECT_SOURCE_DIR}/tests")

add_python_test(job)
add_python_test(cluster)
add_python_test(key)
add_python_test(transport)
add_python_test(aws_key)
add_python_test(trad_cluster)
add_python_test(sge)
add_python_test(pbs)
add_python_test(slurm)
add_python_test(download)
add_python_test(taskflow)
add_python_test(ansible_inventory)
# For now ansible only supports Python2
add_python_test(ansible_run_log PY2_ONLY)
add_python_test(cloud_provider)
