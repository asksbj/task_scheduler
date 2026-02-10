import datetime

from core.database import db_manager


class Task:
    
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.task_type = kwargs.get('task_type')
        self.owner = kwargs.get('owner')
        self.last_update = kwargs.get('last_update') or datetime.datetime.now()
        self.start_time = kwargs.get('start_time')
        self.end_time = kwargs.get('end_time')

    @classmethod
    def from_db(cls, row: tuple) -> 'Task':
        if not row:
            return None

        columns = ['id', 'task_type', 'owner', 'last_update', 'start_time', 'end_time']
        data = dict(zip(columns, row))
        return cls(**data)

    @classmethod
    def get_task_by_id(cls, task_id: int) -> 'Task':
        if not task_id:
            return None

        query = "SELECT * FROM task WHERE id = %s"
        params = (task_id, )

        row = db_manager.execute_query(query, params, fetch_one=True)
        task = cls.from_db(row)
        return task


    def save(self) -> None:
        if self.id:
            self._update()
        else:
            self._insert()
            print(f"Task id after save {self.id}")

    def _insert(self) -> None:
        query = "INSERT INTO task (task_type, owner, last_update, start_time, end_time) VALUES (%s, %s, %s, %s, %s)"
        params = (self.task_type, self.owner, self.last_update, self.start_time, self.end_time)

        self.id = db_manager.execute_query(query, params)

    
