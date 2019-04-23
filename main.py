import random
from arango import ArangoClient
import datetime


def init():
    client = ArangoClient()
    sys_db = client.db('_system', username='root', password='1488')
    sys_db.databases()
    if not sys_db.has_database('project'):
        sys_db.create_database('project')

    global db
    db = client.db('project', username='root', password='1488')
    if not db.has_collection('User'):
        db.create_collection(name='User')
    global users
    users = db.collection('User')

    if not db.has_collection('Projects'):
        db.create_collection(name='Projects')
    global projects
    projects = db.collection('Projects')

    if not db.has_collection('Tasks'):
        db.create_collection(name='Tasks')
    global tasks
    tasks = db.collection('Tasks')

    global project_user
    if db.has_graph('Project_User'):
        project_user = db.graph('Project_User')
    else:
        project_user = db.create_graph('Project_User')

    global task_user
    if db.has_graph('Task_User'):
        task_user = db.graph('Task_User')
    else:
        task_user = db.create_graph('Task_User')

    global task_project
    if db.has_graph('Task_Project'):
        task_project = db.graph('Task_Project')
    else:
        task_project = db.create_graph('Task_Project')

    global edge_w_o_p
    if project_user.has_edge_definition('Project_User'):
        edge_w_o_p = project_user.edge_collection('Project_User')
    else:
        edge_w_o_p = project_user.create_edge_definition(
            edge_collection='Project_User',
            from_vertex_collections=['User'],
            to_vertex_collections=['Projects']
        )

    global edge_w_o_t
    if task_user.has_edge_definition('Task_User'):
        edge_w_o_t = task_user.edge_collection('Task_User')
    else:
        edge_w_o_t = task_user.create_edge_definition(
            edge_collection='Task_User',
            from_vertex_collections=['User'],
            to_vertex_collections=['Tasks']
        )

    global edge_t_w_p
    if task_project.has_edge_definition('Task_Project'):
        edge_t_w_p = task_project.edge_collection('Task-Project')
    else:
        edge_t_w_p = task_project.create_edge_definition(
            edge_collection='Task_Project',
            from_vertex_collections=['Projects'],
            to_vertex_collections=['Tasks']
        )
        generate_data()


def generate_data():
    for i in range(100):
        add_user('name' + str(i), 'mail' + str(i) + '@mail.ma')
    for i in range(100):
        add_project('nameOfProject' + str(i), 'project_key' + str(i))

    for i in range(1, 99):
        user = users.get({'_key': 'mail' + str(i) + '@mail.ma'})
        for x in range(random.randint(2, 4)):
            j = random.randint(1, 99)
            project = projects.get({'_key': 'project_key' + str(j)})
            insert_works_on_project(user, project)

    for i in range(1, 99):
        project = projects.get({'_key': 'project_key' + str(i)})
        for x in range(random.randint(2, 4)):
            create_task_for_project(project, project['name'] + '_' + str(x),
                                    str(datetime.datetime(2019, 3, 24, x + 1,
                                                          random.choice([0, 30]), 0)),
                                    str(datetime.datetime(2019, 4, 25,
                                                          x + 1, random.choice([0, 30]), 0)), 'in process')

    for i in range(1, 99):
        user = users.get({'_key': 'mail' + str(i) + '@mail.ma'})
        projs = list_projects_on_user(user)
        for proj in projs:
            tasks_proj = list_tasks_of_project(proj)
            x = random.randint(0, len(tasks_proj))
            for j in range(x):
                s = random.randint(1, x) - 1
                insert_works_on_task(user, tasks_proj[s])


def add_user(name, email):
    user = db.insert_document('User', {'_key': email, 'name': name, 'email': email})
    return user


def add_project(name, project_key):
    project = db.insert_document('Projects', {'_key': project_key, 'name': name})
    return project


def add_task(name, start, end, status):
    task = db.insert_document('Tasks', {'_key': name.strip(' '), 'name': name, 'start': start,
                                        'end': end, 'status': status})
    return task


def insert_works_on_project(user, project):
    edge_w_o_p.insert({
        '_from': user['_id'],
        '_to': project['_id']
    })


def insert_works_on_task(user, task):
    edge_w_o_t.insert({
        '_from': user['_id'],
        '_to': task['_id']
    })


def insert_task_to_project(project, task):
    edge_t_w_p.insert({
        '_from': project['_id'],
        '_to': task['_id']
    })


def list_users_on_task(task):
    trav = task_user.traverse(
        start_vertex=task['_id'],
        direction='inbound',
        strategy='bfs',
        edge_uniqueness='global',
        vertex_uniqueness='global',
    )
    return trav['vertices'][1:]


def list_tasks_on_user(user):
    trav = task_user.traverse(
        start_vertex=user['_id'],
        direction='outbound',
        strategy='bfs',
        edge_uniqueness='global',
        vertex_uniqueness='global',
    )
    return trav['vertices'][1:]


def list_projects_on_user(user):
    trav = project_user.traverse(
        start_vertex=user['_id'],
        direction='outbound',
        strategy='bfs',
        edge_uniqueness='global',
        vertex_uniqueness='global',
    )
    return trav['vertices'][1:]


def list_users_on_project(project):
    trav = project_user.traverse(
        start_vertex=project['_id'],
        direction='inbound',
        strategy='bfs',
        edge_uniqueness='global',
        vertex_uniqueness='global',
    )
    return trav['vertices'][1:]


def list_tasks_of_project(proj):
    trav = task_project.traverse(
        start_vertex=proj['_id'],
        direction='outbound',
        strategy='bfs',
        edge_uniqueness='global',
        vertex_uniqueness='global',
    )
    return trav['vertices'][1:]


def create_task_for_project(project, name, start, end, status):
    task = add_task(name, start, end, status)
    insert_task_to_project(project, task)


def list_unfinished_tasks(proj):
    """
        Lists tasks that are not finished in project.
        :param proj: full JSON representation; not just id
        :return: list of JSON representations of tasks
    """
    trav = list_tasks_of_project(proj)
    arr = []
    for task in trav:
        if task['status'] != 'finished':
            arr.append(task)
    return arr


def list_tasks_finished_by(user):
    """
        Lists tasks taht user finished
        :param user: full JSON representation; not just id
        :return: list of JSON representations of tasks
    """
    trav = list_tasks_on_user(user)
    arr = []
    for task in trav:
        if task['status'] == 'finished':
            arr.append(task)
    return arr


def list_deadline_missed_user(user):
    """
    Lists users tasks that went over deadline
    :param user: full JSON representation; not just id
    :return: list of JSON representations of tasks
    """
    trav = list_tasks_on_user(user)
    arr = []
    for task in trav:
        if task['status'] == 'deadline' or check_deadline_task(task):
            arr.append(task)
    return arr


def list_deadline_missed_project(project):
    """
    Lists task for project where task went over deadline
    :param project: full JSON representation; not just id
    :return: list of JSON representations of tasks
    """
    trav = list_tasks_of_project(project)
    arr = []
    for task in trav:
        if task['status'] == 'deadline' or check_deadline_task(task):
            arr.append(task)
    return arr


def find_deadline_user_missed_on_proj(project):
    """
    Finds users that missed deadline for their tasks.
    :param project: JSON representing project
    :return: list of users
    """
    missed_tasks = list_deadline_missed_project(project)
    arr = []
    for task in missed_tasks:
        trav = list_users_on_task(task)
        for tra in trav:
            if not tra in arr:
                arr.append(tra)
    return arr


def change_status(task, status):
    task['status'] = status
    tasks.update(task)


def find_close_to_deadline(project, time):
    tasks = list_tasks_of_project(project)
    arr = []
    for task in tasks:
        if time_till_deadline(task) < time:
            arr.append(task)
    return arr


def remove_user_from_project(user, project):
    u_tasks = list_tasks_on_user(user)
    p_tasks = list_tasks_of_project(project)
    taskss = []
    for i in u_tasks:
        if i in p_tasks:
            taskss.append(i)
    for task in taskss:
        remove_user_from_task(user, task)


def remove_user_from_task(user, task):
    db.aql.execute('FOR edge in Task_User Filter edge._from == "' + user['_id']
                   + '" and edge._to =="' + task['_id'] + '" remove edge in Task_User')


def check_deadline_task(task):
    if datetime.datetime.now() >= datetime.datetime.strptime(task['end'], "%Y-%m-%d %H:%M:%S"):
        task['status'] = 'deadline'
        db.update_document(task)
        return True
    return False


def time_till_deadline(task):
    return datetime.datetime.strptime(task['end'], "%Y-%m-%d %H:%M:%S") - datetime.datetime.now()


def time_interval(task):
    return datetime.datetime.strptime(task['end'], "%Y-%m-%d %H:%M:%S") - \
           datetime.datetime.strptime(task['start'], "%Y-%m-%d %H:%M:%S")


init()
user = users.get({'_key': 'mail45@mail.ma'})
proj = projects.get({'_key': 'project_key48'})
print(find_close_to_deadline(proj, datetime.timedelta(days=1)))
