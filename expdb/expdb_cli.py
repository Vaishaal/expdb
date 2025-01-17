import datetime

import click
import dateparser

from expdb import expdb

db = expdb.ExpDB()


@click.group()
def cli():
    pass


@cli.group()
def hide():
    pass


@hide.command(name='experiments')
@click.option('-a', '--all', '_all', type=str, default=None, is_flag=True)
@click.option('--uuid', type=str, default=None)
@click.option('--uuid_list', type=str, default=None)
@click.option('--before', type=str, default=None)
@click.option('--after', type=str, default=None)
def hide_experiments(_all, uuid, uuid_list, before, after):
    if uuid_list is not None:
        assert uuid is None
        for cur_uuid in uuid_list.split(','):
            db.hide_experiment(cur_uuid)
            print(f'evaluation {cur_uuid} is now hidden')
    else:
        exps = db.get_experiments(show_hidden=False)
        if after is not None:
            after_datetime = dateparser.parse(
                after, settings={'RETURN_AS_TIMEZONE_AWARE': True})
            exps = [
                x for x in exps if x.creation_time.replace(
                    tzinfo=datetime.timezone.utc) >= after_datetime
            ]
        if before is not None:
            before_datetime = dateparser.parse(
                before, settings={'RETURN_AS_TIMEZONE_AWARE': True})
            exps = [
                x for x in exps if x.creation_time.replace(
                    tzinfo=datetime.timezone.utc) <= before_datetime
            ]
        if after is None and before is None:
            assert _all

        num_hidden = 0
        for m in exps:
            db.hide_experiment(m.uuid)
            num_hidden += 1
        if num_hidden > 0:
            print(f'Hid {num_hidden} evaluations')
        else:
            print('No evaluations to hide')




@cli.group()
def list():
    pass


@list.command(name='experiments')
@click.option('--show_hidden', is_flag=True)
@click.option('--show_data', is_flag=True)
@click.option('--filter_fields', type=str, default=None)
@click.option('--uuid', type=str, default=None)
@click.option('--name_filter', type=str, default=None)
@click.option('--after', type=str, default=None)
@click.option('--before', type=str, default=None)
@click.option('--project', type=str, default=None)
def list_experiments(show_hidden, show_data, filter_fields, uuid, name_filter,
                     after, before, project):
    exps = db.get_experiments(show_hidden=show_hidden)
    exps = sorted(exps, key=lambda x: x.creation_time)
    if after is not None:
        after_datetime = dateparser.parse(
            after, settings={'RETURN_AS_TIMEZONE_AWARE': True})
        exps = [
            x for x in exps if x.creation_time.replace(
                tzinfo=datetime.timezone.utc) >= after_datetime
        ]
    if before is not None:
        before_datetime = dateparser.parse(
            before, settings={'RETURN_AS_TIMEZONE_AWARE': True})
        exps = [
            x for x in exps if x.creation_time.replace(
                tzinfo=datetime.timezone.utc) <= before_datetime
        ]
    if name_filter is not None:
        exps = [x for x in exps if name_filter in x.name]

    if uuid is not None:
        exp = [x for x in exp if str(uuid) == str(x.uuid)]

    for m in exps:
        if project is not None:
            if m.project_name != project:
                continue
        print(
            f'{m.uuid} : creation_time {m.creation_time}  name {m.name} tags: {m.tags}',
            end='')
        if show_hidden:
            print(f'\thidden {m.hidden}')
        else:
            print()
        if show_data:
            print(f'\tDescription: {m.description}')
            if show_data:
                keys_to_show = None
                if filter_fields is not None:
                    keys_to_show = filter_fields.split(',')
                print('\tExperiment :')
                if isinstance(m.data, dict):
                    sorted_keys = sorted(m.data.keys())
                    for k in sorted_keys:
                        if keys_to_show is not None and k not in keys_to_show:
                            continue
                        print(f'\t\t{k}: {m.data[k]}')
                else:
                    print("\t\t" + str(m.data))
            print()


@list.command(name='projects')
@click.option('--show_hidden', is_flag=True)
@click.option('--show_data', is_flag=True)
@click.option('--filter_fields', type=str, default=None)
@click.option('--uuid', type=str, default=None)
@click.option('--name_filter', type=str, default=None)
@click.option('--after', type=str, default=None)
@click.option('--before', type=str, default=None)
def list_projects(show_hidden, show_data, filter_fields, uuid, name_filter,
                  after, before):
    projs = db.get_projects(show_hidden=show_hidden)
    projs = sorted(projs, key=lambda x: x.creation_time)
    if after is not None:
        after_datetime = dateparser.parse(
            after, settings={'RETURN_AS_TIMEZONE_AWARE': True})
        projs = [
            x for x in projs if x.creation_time.replace(
                tzinfo=datetime.timezone.utc) >= after_datetime
        ]
    if before is not None:
        before_datetime = dateparser.parse(
            before, settings={'RETURN_AS_TIMEZONE_AWARE': True})
        projs = [
            x for x in projs if x.creation_time.replace(
                tzinfo=datetime.timezone.utc) <= before_datetime
        ]
    if name_filter is not None:
        projs = [x for x in projs if name_filter in x.name]

    if uuid is not None:
        exp = [x for x in exp if str(uuid) == str(x.uuid)]

    for m in projs:
        print(f'{m.name} : creation_time {m.creation_time}: {m.tags}', end='')
        if show_hidden:
            print(f'\thidden {m.hidden}')
        else:
            print()
        if show_data and len(m.data) > 0:
            print(f'\tDescription: {m.description}')
            keys_to_show = None
            if filter_fields is not None:
                keys_to_show = filter_fields.split(',')
            print('\tProject:')
            if isinstance(m.data, dict):
                sorted_keys = sorted(m.data.keys())
                for k in sorted_keys:
                    if keys_to_show is not None and k not in keys_to_show:
                        continue
                    print(f'\t\t{k}: {m.data[k]}')
            else:
                print("\t\t" + str(m.data))
            print()


@list.command(name='experiment_states')
@click.option('--show_hidden', is_flag=True)
@click.option('--show_data', is_flag=True)
@click.option('--filter_fields', type=str, default=None)
@click.option('--uuid', type=str, default=None)
@click.option('--name_filter', type=str, default=None)
@click.option('--after', type=str, default=None)
@click.option('--before', type=str, default=None)
def list_experiment_states(show_hidden, show_data, filter_fields, uuid,
                           name_filter, after, before):
    states = db.get_experiment_states(show_hidden=show_hidden)
    states = sorted(states, key=lambda x: x.creation_time)
    if after is not None:
        after_datetime = dateparser.parse(
            after, settings={'RETURN_AS_TIMEZONE_AWARE': True})
        states = [
            x for x in states if x.creation_time.replace(
                tzinfo=datetime.timezone.utc) >= after_datetime
        ]
    if before is not None:
        before_datetime = dateparser.parse(
            before, settings={'RETURN_AS_TIMEZONE_AWARE': True})
        states = [
            x for x in states if x.creation_time.replace(
                tzinfo=datetime.timezone.utc) <= before_datetime
        ]
    if name_filter is not None:
        states = [x for x in states if name_filter in x.name]

    if uuid is not None:
        exp = [x for x in exp if str(uuid) == str(x.uuid)]

    for m in states:
        print(
            f'{m.uuid} : creation_time {m.creation_time}  name {m.name} tags: {m.tags}',
            end='')
        if show_hidden:
            print(f'\thidden {m.hidden}')
        else:
            print()
        if show_data and len(m.data) > 0:
            print(f'\tDescription: {m.description}')
            keys_to_show = None
            if filter_fields is not None:
                keys_to_show = filter_fields.split(',')
            print('\Experiment State:')
            if isinstance(m.data, dict):
                sorted_keys = sorted(m.data.keys())
                for k in sorted_keys:
                    if keys_to_show is not None and k not in keys_to_show:
                        continue
                    print(f'\t\t{k}: {m.data[k]}')
            else:
                print("\t\t" + str(m.data))
            print()


if __name__ == '__main__':
    cli()
