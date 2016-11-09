from __future__ import print_function
import click
from cumulus.scripts.command import (cli, pass_proxy,
                                     create_profile,
                                     delete_profile)
import functools
import sys

test_failures = []

def report():
    if len(test_failures) == 0:
        print("\nAll tests passed")
        sys.exit(0)
    else:
        print("\nTest failures present!")
        for test in test_failures:
            print("    {}".format(test))
        sys.exit(1)


def test_case(func):
    @functools.wraps(func)
    def _catch_exceptions(*args, **kwargs):
        try:
            ctx, proxy = args[0], args[1]
            if proxy.verbose >= 1:
                print('%s...' % func.__name__, end='')

            func(*args, **kwargs)

            if proxy.verbose >= 1:
                print('OK')
            else:
                print('.', end='')

        except AssertionError as e:
            test_failures.append(func.__name__)
            if proxy.verbose >= 1:
                print('ERROR')

                if proxy.verbose >= 2:
                    import traceback
                    traceback.print_exc()
            else:
                print('E', end='')

    _catch_exceptions = click.pass_context(_catch_exceptions)
    _catch_exceptions = pass_proxy(_catch_exceptions)

    return _catch_exceptions


@cli.command()
@click.option('--profile_section', default='profile')
@test_case
def test_profile(ctx, proxy, profile_section):
    assert len(proxy.profiles) == 0, \
        'Profile already exist!'

    ctx.invoke(create_profile, profile_section=profile_section)

    profiles = proxy.get('user/%s/aws/profiles' % proxy.user['_id'])
    assert len(profiles) == 1, \
        "After create_profile only one profile should exist"

    ctx.invoke(delete_profile, profile_section=profile_section)

    profiles = proxy.get('user/%s/aws/profiles' % proxy.user['_id'])
    assert len(profiles) == 0, \
        "After delete_profile no profiles should exist"


if __name__ == '__main__':
    try:
        cli()
    except SystemExit:
        pass

    report()
