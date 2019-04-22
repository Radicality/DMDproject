import random

from arango import ArangoClient

client = ArangoClient()


def init():
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
    if db.has_graph('Project-User'):
        project_user = db.graph('Project-User')
    else:
        project_user = db.create_graph('Project-User')

    global task_user
    if db.has_graph('Task-User'):
        task_user = db.graph('Task-User')
    else:
        task_user = db.create_graph('Task-User')

    global task_project
    if db.has_graph('Task-Project'):
        task_project = db.graph('Task-Project')
    else:
        task_project = db.create_graph('Task-Project')

    global edge_w_o_p
    if project_user.has_edge_definition('Project-User'):
        edge_w_o_p = project_user.edge_collection('Project-User')

    else:
        edge_w_o_p = project_user.create_edge_definition(
            edge_collection='Project-User',
            from_vertex_collections=['User'],
            to_vertex_collections=['Projects']
        )

    global edge_w_o_t
    if task_user.has_edge_definition('Task-User'):
        edge_w_o_t = task_user.edge_collection('Task-User')
    else:
        edge_w_o_t = task_user.create_edge_definition(
            edge_collection='Task-User',
            from_vertex_collections=['User'],
            to_vertex_collections=['Tasks']
        )

    global edge_t_w_p
    if task_project.has_edge_definition('Task-Project'):
        edge_t_w_p = task_project.edge_collection('Task-Project')
    else:
        edge_t_w_p = task_project.create_edge_definition(
            edge_collection='Task-Project',
            from_vertex_collections=['User'],
            to_vertex_collections=['Tasks']
        )

    generate()

def generate():
    for i in range(100):
        add_user('name'+str(i), 'mail'+str(i)+'@mail.ma')
    for i in range(100):
        add_project('nameOfProject'+str(i), 'project_key'+str(i))
    for i in range(100):
        add_task('nameOfTask'+str(i), str(i//24)+'-00', str((i+1)//24)+'-00', 'in process')
    for i in range(1, 99):
        user = users.get({'_key': 'mail'+str(i)+'@mail.ma'})
        for x in range(random.randint(2, 4)):
            j = random.randint(1, 99)
            project = projects.get({'_key': 'project_key'+str(j)})
            insert_works_on_project(user['_id'], project['_id'])
    for i in range(1, 99):
        user = users.get({'_key': 'mail'+str(i)+'@mail.ma'})
        for x in range(random.randint(2, 4)):
            j = random.randint(1, 99)
            project = tasks.get({'_key': 'nameOfTask'+str(j)})
            insert_works_on_task(user['_id'], project['_id'])

def add_user(name, email):
    user = db.insert_document('User', {'_key': email, 'name': name, 'email': email})
    return user


def add_project(name, project_key):
    project = db.insert_document('Projects', {'_key': project_key, 'name': name})
    return project


def add_task(name, start, end, status):
    task = db.insert_document('Tasks', {'_key': name.strip(' '), 'name': name, 'start': start, 'end': end, 'status': status})
    return task


def insert_works_on_project(user, project):
    edge_w_o_p.insert({
        '_from': user,
        '_to': project
    })


def insert_works_on_task(user, task):
    edge_w_o_t.insert({
        '_from': user,
        '_to': task
    })


def traverse_works_on_tasks():
    trav = task_user.traverse(
        start_vertex='User/mail6@mail.ma',
        direction='outbound',
        strategy='bfs',
        edge_uniqueness='global',
        vertex_uniqueness='global',
    )
    return trav['vertices'][1:]


def traverse_works_on_project():
    trav = project_user.traverse(
        start_vertex='User/mail6@mail.ma',
        direction='outbound',
        strategy='bfs',
        edge_uniqueness='global',
        vertex_uniqueness='global',
    )
    return trav['vertices'][1:]

init()
print(traverse_works_on_tasks())
