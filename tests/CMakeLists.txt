# Set this variable to the minimum coverage percentage to be considered passing
#set(COVERAGE_MINIMUM_PASS 90)
#set(JS_COVERAGE_MINIMUM_PASS 75)

python_tests_init()

add_python_style_test(flake8_cumulus "${PROJECT_SOURCE_DIR}/cumulus")
#add_python_style_test(flake8_cumulus_tests "${PROJECT_SOURCE_DIR}/tests")
