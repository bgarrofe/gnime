from pydantic import BaseModel


class CSVWriterContext(BaseModel):

    file_path: str
    delimiter: str = ','
    header: bool = True
    index: bool = False
