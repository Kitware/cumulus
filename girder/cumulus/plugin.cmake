add_python_test(starclusterconfig PLUGIN cumulus RESOURCE_LOCKS cherrypy)
add_python_test(job PLUGIN cumulus)
add_python_test(cluster PLUGIN cumulus)
add_python_test(script PLUGIN cumulus)
add_python_test(volume PLUGIN cumulus)
add_python_style_test(python_static_analysis_cumulus "${PROJECT_SOURCE_DIR}/plugins/cumulus/server")
