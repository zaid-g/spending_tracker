from pydantic import BaseModel, validator


class Paths(BaseModel):
    root_data_folder_path: str
    raw_data_folder_path: str
    cleaned_data_folder_path: str
    historical_categorized_data_file_path: str

    @validator("root_data_folder_path")
    def root_data_folder_path_must_exist(cls, v):
        if not os.path.exists(v):
            raise ValueError("Folder {v} does not exist.")
        return v.title()

    @validator("raw_data_folder_path")
    def raw_data_folder_path_must_exist(cls, v):
        if not os.path.exists(v):
            raise ValueError("Folder {v} does not exist.")
        return v.title()

    @validator("cleaned_data_folder_path")
    def cleaned_data_folder_path_must_exist(cls, v):
        if not os.path.exists(v):
            raise ValueError("Folder {v} does not exist.")
        return v.title()
