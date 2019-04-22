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
    projects = db.collection('Project')

    if not db.has_collection('Tasks'):
        db.create_collection(name='Tasks')
    global tasks
    tasks = db.collection('Tasks')

    global project_graph
    if db.has_graph('Project'):
        project_graph = db.graph('Project')
    else:
        project_graph = db.create_graph('Project')

    global edge_w_o_p
    if project_graph.has_edge_definition('Project'):
        edge_w_o_p = project_graph.edge_collection('Project')

    else:
        edge_w_o_p = project_graph.create_edge_definition(
            edge_collection='Project',
            from_vertex_collections=['User'],
            to_vertex_collections=['Projects']
        )

    global edge_w_o_t
    if project_graph.has_edge_definition('Task'):
        edge_w_o_t = project_graph.edge_collection('Task')
    else:
        edge_w_o_t = project_graph.create_edge_definition(
            edge_collection='Task',
            from_vertex_collections=['User'],
            to_vertex_collections=['Tasks']
        )


def add_user(name, email):
    user = db.insert_document('User', {'_key': email, 'name': name, 'email': email})
    return user


def add_project(name, project_key):
    project = db.insert_document('Projects', {'_key': project_key, 'name': name})
    return project


def add_task(name, start, end, status):
    task = db.insert_document('Tasks', {'name': name, 'start': start, 'end': end, 'status': status})
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

init()
us = add_user('name', 's10@r.v')
task = add_task('do', '23-40', '23-45', 'in process')
insert_works_on_task(us['_id'], task['_id'])
