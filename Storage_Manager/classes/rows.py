class Rows:
    def __init__(self, data: any = None, rows_count: int = 0, idx : list[int] = None) -> None:
        self.data = data if data is not None else []
        self.rows_count = rows_count
        self.idx = idx if idx is not None else []