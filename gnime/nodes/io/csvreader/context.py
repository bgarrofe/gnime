from pydantic import BaseModel


class CSVReaderContext(BaseModel):

    file_path: str
    delimiter: str = ','
    header: int = 0
    columns: list = None
