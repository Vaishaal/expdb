import contextlib
import getpass
import hashlib
import io
import json
import pathlib
import pickle
import uuid
from datetime import datetime, timezone

import numpy as np
import sqlalchemy as sqla
from sqlalchemy.ext.declarative import \
    declarative_base as sqla_declarative_base
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy_utils import database_exists, create_database

sqlalchemy_base = sqla_declarative_base()


def gen_short_uuid(num_chars=None):
    num = uuid.uuid4().int
    alphabet = '23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    res = []
    while num > 0:
        num, digit = divmod(num, len(alphabet))
        res.append(alphabet[digit])
    res2 = ''.join(reversed(res))
    if num_chars is None:
        return res2
    else:
        return res2[:num_chars]

def is_jsonable(x):
    try:
        json.dumps(x)
        return True
    except:
        return False


class ExperimentState(sqlalchemy_base):
    __tablename__ = 'experiment_state'
    uuid = sqla.Column(sqla.String, primary_key=True)
    name = sqla.Column(sqla.String, unique=True)
    tags = sqla.Column(sqla.String)
    data = sqla.Column(sqla.JSON)
    experiment_uuid = sqla.Column(sqla.String,
                                  sqla.ForeignKey('experiment.uuid'),
                                  nullable=True)
    description = sqla.Column(sqla.String)
    creation_time = sqla.Column(sqla.DateTime(timezone=False),
                                server_default=sqla.sql.func.now())
    experiment = sqla.orm.relationship('Experiment',
                                       back_populates='states',
                                       foreign_keys=[experiment_uuid])
    hidden = sqla.Column(sqla.Boolean)

    def __repr__(self):
        return f'<ExperimentState(uuid="{self.uuid}", name="{self.name}", tags="{self.tags}",  creation_time="{self.creation_time}", hidden="{self.hidden}")>'


class Experiment(sqlalchemy_base):
    __tablename__ = 'experiment'
    uuid = sqla.Column(sqla.String, primary_key=True)
    name = sqla.Column(sqla.String, unique=True)
    tags = sqla.Column(sqla.String)
    data = sqla.Column(sqla.JSON)
    project_name = sqla.Column(sqla.String,
                               sqla.ForeignKey('project.name'),
                               nullable=True)
    description = sqla.Column(sqla.String)
    creation_time = sqla.Column(sqla.DateTime(timezone=False),
                                server_default=sqla.sql.func.now())
    project = sqla.orm.relationship('Project',
                                    back_populates='experiments',
                                    foreign_keys=[project_name])
    states = sqla.orm.relationship(
        'ExperimentState',
        back_populates='experiment',
        cascade='all, delete, delete-orphan',
        foreign_keys='ExperimentState.experiment_uuid')
    hidden = sqla.Column(sqla.Boolean)

    def __repr__(self):
        return f'<Experiment(uuid="{self.uuid}", name="{self.name}", tags="{self.tags}",  creation_time="{self.creation_time}", hidden="{self.hidden}")>'


class Project(sqlalchemy_base):
    __tablename__ = 'project'
    name = sqla.Column(sqla.String,
                       unique=True,
                       nullable=False,
                       primary_key=True)
    tags = sqla.Column(sqla.String)
    description = sqla.Column(sqla.String)
    data = sqla.Column(sqla.JSON)
    creation_time = sqla.Column(sqla.DateTime(timezone=False),
                                server_default=sqla.sql.func.now())
    experiments = sqla.orm.relationship('Experiment',
                                        back_populates='project',
                                        cascade='all, delete, delete-orphan',
                                        foreign_keys='Experiment.project_name')
    hidden = sqla.Column(sqla.Boolean)

    def __repr__(self):
        return f'<Project(name="{self.name}", tags="{self.tags}", creation_time="{self.creation_time}", hidden="{self.hidden}")>'


class ExpDB(object):
    def __init__(self,
                 db_connection_string="sqlite:///expdb.db",
                 sql_verbose=False):
        self.sql_verbose = sql_verbose
        self.db_connection_string = db_connection_string
        self.engine = sqla.create_engine(self.db_connection_string,
                                         echo=self.sql_verbose)
        self.sessionmaker = sqla.orm.sessionmaker(bind=self.engine,
                                                  expire_on_commit=False)
        self.uuid_length = 10
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
            sqlalchemy_base.metadata.create_all(bind=self.engine)

    @contextlib.contextmanager
    def session_scope(self):
        session = self.sessionmaker()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def gen_short_uuid(self):
        new_id = gen_short_uuid(num_chars=self.uuid_length)
        # TODO: check that we don't have a collision with the db?
        return new_id

    def run_query_with_optional_session(self, query, session=None):
        if session is None:
            with self.session_scope() as sess:
                return query(sess)
        else:
            return query(session)

    def run_get(self, uuid, get_fn, session=None, assert_exists=True):
        assert isinstance(uuid, str)

        def query(sess):
            result = get_fn(sess)
            assert len(result) <= 1
            if assert_exists:
                assert len(result) == 1
            if len(result) == 0:
                return None
            else:
                return result[0]

        return self.run_query_with_optional_session(query, session)

    def create_project(self,
                       *,
                       name,
                       data,
                       description=None,
                       tags=None,
                       verbose=False):
        assert isinstance(name, str)
        assert is_jsonable(data)
        with self.session_scope() as session:
            new_project = Project(name=name,
                                  description=description,
                                  tags=tags,
                                  data=data,
                                  hidden=False)
            session.add(new_project)
        return self.get_project(name=name, assert_exists=True)

    def get_project(self, name=None, *, session=None, assert_exists=True):
        def get_fn(sess):
            return self.get_projects(names=[name], session=sess)

        return self.run_get(name,
                            get_fn,
                            session=session,
                            assert_exists=assert_exists)

    def get_projects(self, names=None, *, session=None, show_hidden=False):
        cur_options = []
        cur_options.append(
            sqla.orm.subqueryload(Project.experiments).subqueryload(
                Experiment.states))

        filter_list = []
        if not show_hidden:
            filter_list.append(Project.hidden == False)

        if names is not None:
            filter_list.append(Project.name.in_(names))

        def query(sess):
            return sess.query(Project).options(cur_options).filter(
                *filter_list).all()

        return self.run_query_with_optional_session(query, session)


    def create_experiment(self,
                          *,
                          project_name,
                          data,
                          name=None,
                          description=None,
                          tags=None,
                          verbose=False):
        assert isinstance(project_name, str)
        assert is_jsonable(data)

        with self.session_scope() as session:
            new_id = self.gen_short_uuid()
            new_exp = Experiment(uuid=new_id,
                                 project_name=project_name,
                                 name=name,
                                 description=description,
                                 tags=tags,
                                 data=data,
                                 hidden=False)
            session.add(new_exp)
        return self.get_experiment(uuid=new_id, assert_exists=True)

    def update_project_data(self, *, name, data):
        assert isinstance(name, str)
        assert is_jsonable(data)
        with self.session_scope() as session:
            project = self.get_project(name,
                                       session=session,
                                       assert_exists=True)
            project.data.update(data)
            flag_modified(project, "data")
            session.add(project)
            session.commit()

    def update_experiment_data(self, *, uuid, data):
        assert isinstance(uuid, str)
        assert is_jsonable(data)
        with self.session_scope() as session:
            experiment = self.get_experiment(uuid,
                                             session=session,
                                             assert_exists=True)
            experiment.data.update(data)
            flag_modified(experiment, "data")
            session.add(experiment)
            session.commit()

    def update_experiment_state_data(self, *, uuid, data):
        assert isinstance(uuid, str)
        assert is_jsonable(data)
        with self.session_scope() as session:
            state = self.get_experiment_state(uuid,
                                              session=session,
                                              assert_exists=True)
            state.data.update(data)
            flag_modified(state, "data")
            session.add(state)
            session.commit()

    def get_experiment(self, uuid=None, *, session=None, assert_exists=True):
        def get_fn(sess):
            return self.get_experiments(uuids=[uuid], session=sess)

        return self.run_get(uuid,
                            get_fn,
                            session=session,
                            assert_exists=assert_exists)

    def get_experiments(self, uuids=None, *, session=None, show_hidden=False):
        cur_options = []
        cur_options.append(sqla.orm.subqueryload(Experiment.states))

        filter_list = []
        if not show_hidden:
            filter_list.append(Experiment.hidden == False)

        if uuids is not None:
            filter_list.append(Experiment.uuid.in_(uuids))

        def query(sess):
            return sess.query(Experiment).options(cur_options).filter(
                *filter_list).all()

        return self.run_query_with_optional_session(query, session)

    def create_experiment_state(self,
                                *,
                                experiment_uuid,
                                data,
                                name=None,
                                description=None,
                                tags=None,
                                verbose=False):
        assert isinstance(experiment_uuid, str)
        assert is_jsonable(data)

        with self.session_scope() as session:
            new_id = self.gen_short_uuid()
            new_exp = Experiment(uuid=new_id,
                                 experiment_uuid=experiment_uuid,
                                 name=name,
                                 description=description,
                                 tags=tags,
                                 data=data,
                                 hidden=False)
            session.add(new_exp)
        return self.get_experiment_state(uuid=new_id, assert_exists=True)

    def get_experiment_state(self,
                             uuid=None,
                             *,
                             session=None,
                             assert_exists=True):
        def get_fn(sess):
            return self.get_experiment_states(uuids=[uuid], session=sess)

        return self.run_get(uuid,
                            get_fn,
                            session=session,
                            assert_exists=assert_exists)

    def get_experiment_states(self,
                              uuids=None,
                              *,
                              session=None,
                              show_hidden=False):
        cur_options = []
        filter_list = []

        if not show_hidden:
            filter_list.append(ExperimentState.hidden == False)

        if uuids is not None:
            filter_list.append(ExperimentState.uuid.in_(uuids))

        def query(sess):
            return sess.query(ExperimentState).options(cur_options).filter(
                *filter_list).all()

        return self.run_query_with_optional_session(query, session)

    def hide_experiment_state(self, experiment_state_uuid):
        with self.session_scope() as session:
            state = self.get_experiment_state(experiment_state_uuid,
                                             session=session,
                                             assert_exists=True)
            state.hidden = True

    def hide_project(self, project_name):
        with self.session_scope() as session:
            proj = self.get_project(project_name,
                                    session=session,
                                    assert_exists=True)
            proj.hidden = True

    def hide_experiment(self, experiment_uuid):
        with self.session_scope() as session:
            exp = self.get_experiment(experiment_uuid,
                                       session=session,
                                       assert_exists=True)
            exp.hidden = True




if __name__ == "__main__":
    pass
