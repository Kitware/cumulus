#!/bin/sh
#                             _
#                            | |
#   ___ _   _ _ __ ___  _   _| |_   _ ___
#  / __| | | | '_ ` _ \| | | | | | | / __|
# | (__| |_| | | | | | | |_| | | |_| \__ \
#  \___|\__,_|_| |_| |_|\__,_|_|\__,_|___/
#
{% include "schedulers/" + cluster.config.scheduler.type + ".sh" -%}

{% for command in job.commands %}
{{ command -}}
{% endfor %}


