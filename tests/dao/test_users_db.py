import time
from datetime import datetime
from sqlalchemy import select
from src.dao.users_db import User, UserDatabase, Request, RequestTypesEnum, RequestPlatformsEnum
from tests.conftest import *


@pytest.fixture(scope='function')
def users_test_objs(db_session) -> list[User]:
    """
    populates the database with test chats
    :param db_session: generator of session object
    :return: list of created chats
    """
    test_users = [
        User(user_id='123456', username='some_username')
    ]
    add_test_objs_to_db(db_session, test_users)
    return test_users


class TestUserDatabase:

    def test_check_user(self, db_session):
        udb = UserDatabase(db_session())
        user_to_check = User(user_id='123456', username='some_username')

        # test that check_user() adds new user to database
        udb.check_user(user_to_check)
        users_from_db = udb.session.execute(select(User)).scalars().all()
        assert len(users_from_db) == 1
        assert users_from_db[0].user_id == user_to_check.user_id

        # test that check_user() doesn't add extra user
        udb.check_user(user_to_check)
        users_from_db = udb.session.execute(select(User)).scalars().all()
        assert len(users_from_db) == 1
        assert users_from_db[0].user_id == user_to_check.user_id

    def test_add_new_user_request(self, users_test_objs, db_session):
        test_user = users_test_objs[0]
        request = Request(user_id=test_user.user_id, request_type=RequestTypesEnum.sku,
                          request_platform=RequestPlatformsEnum.telegram,
                          request='lorem ipsum', created_at=datetime.now())

        udb = UserDatabase(db_session())
        udb.add_new_user_request(request)

        requests_from_db = udb.session.execute(select(Request)).scalars().all()
        assert len(requests_from_db) == 1
        assert requests_from_db[0].user_id == test_user.user_id

    def test_update_user_last_interaction(self, users_test_objs, db_session):
        udb = UserDatabase(db_session())
        time_before_update_all = udb.session.execute(select(User.last_interaction_date)).scalars().all()
        assert len(time_before_update_all) == 1
        time_before_update = time_before_update_all[0]
        assert time_before_update is None

        time_before_update = datetime.now()
        time.sleep(0.01)

        udb.update_user_last_interaction(users_test_objs[0].user_id)

        updated_last_interaction_all = udb.session.execute(select(User.last_interaction_date)).scalars().all()
        assert len(updated_last_interaction_all) == 1
        updated_last_interaction = updated_last_interaction_all[0]

        time.sleep(0.01)
        time_after_update = datetime.now()

        assert time_before_update < updated_last_interaction < time_after_update
