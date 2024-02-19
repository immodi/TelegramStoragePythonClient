import requests
from pathlib import Path
import os
import mimetypes
import glob

class StorageClient:
    def __init__(self, api_url:str=None):
        self.api_url = api_url or "https://freetelebot.pythonanywhere.com"

    def add_file(self, file_name: str, file_size: int, file_mime_type: str) -> dict:
        """
        Add a file data to the storage BUT doesn't upload the chunks,

        Parameters:
        - file_name: The name of the file to upload.
        - file_size: The size of the file in bytes.
        - file_mime_type: The MIME type of the file.
        """
        data = {
            "name": file_name,
            "mimeType": file_mime_type,
            "size": file_size
        }

        r = requests.post(self.api_url + '/file', data=data)
        response = r.json()
        return response

    def get_file_data(self, file_name: str) -> dict:
        """
        Get the file data from the storage.

        Parameters:
        - file_name: The name of the file to get the data of.

        Returns:
        - fileId: The ID of the file.
        - fileName: The name of the file.
        - chunksIds: A list of the database IDs of the chunks.
        """
        r = requests.get(self.api_url + '/file', params={
            "fileName": file_name
        })

        response = r.json()
        return response
    
    def get_all_files_data(self) -> dict:
        """
        Get all the files data from the storage.
        """
        r = requests.get(self.api_url)

        response = r.json()
        return response
    
    def split_file(self, input_file_path: str, chunk_size: int, output_directory: str) -> str | None:
        """
        Split a file into chunks.

        Parameters:
        - input_file: The path to the input file.
        - chunk_size: The size of each chunk in bytes.
        - output_directory: The directory where the chunks will be saved.

        Returns:
        - output_directory: The directory where the chunks were saved.
        """

        try:
            with open(input_file_path, 'rb') as file:
                if not Path.exists(Path(output_directory)): os.makedirs(output_directory)
                file_name = os.path.basename(input_file_path)
                
                data = file.read(chunk_size)
                chunk_number = 1

                while data:
                    chunk_filename = f"{output_directory}/{file_name}_{chunk_number}.bin"
                    with open(chunk_filename, 'wb') as chunk_file:
                        chunk_file.write(data)

                    chunk_number += 1
                    data = file.read(chunk_size)
                return output_directory
        except Exception as e: return None

    def merge_chunks(self, chunks_directory: str, file_name: str) -> str:
        """
        Merge chunks back into the original file.

        Parameters:
        - chunk_directory: The directory containing the chunks.
        - output_file: The path to the output file.

        Returns:
        - output_file: The path to the output file.
        """
        try:
            with open(file_name, 'wb') as output_file:
                chunk_number = 1
                chunk_filename = f"{chunks_directory}/{file_name}_{chunk_number}.bin"
                while os.path.exists(chunk_filename):
                    with open(chunk_filename, 'rb') as chunk_file:
                        data = chunk_file.read()
                        output_file.write(data)

                    chunk_number += 1
                    chunk_filename = f"{chunks_directory}/{file_name}_{chunk_number}.bin"
            return file_name
        except Exception as e: return str(e)
    
    def get_file_size_in_bytes(self, file_path: str) -> int:
        """
        Get the size of a file in bytes.

        Parameters:
        - file_path: The path to the file 5to get the size of.
        """
        file_size = os.path.getsize(file_path)
        return file_size
    
    def get_mime_type(self, file_path: str) -> str | None:
        """
        Get the MIME type of a file.

        Parameters:
        - file_path: The path to the file 5to get the size of.
        """
        mime_type, encoding = mimetypes.guess_type(file_path)
        return mime_type

    def upload_file(self, file_path: str) -> dict | None:
        """
        Upload a file to the storage.

        Parameters:
        - file_path: The path to the file to upload.
        """
        file_size = self.get_file_size_in_bytes(file_path)
        file_mime_type = self.get_mime_type(file_path)
        file_name = os.path.basename(file_path)
        
        file_data = self.add_file(file_name, file_size, file_mime_type)
        
        file_db_id = file_data.get("fileId", None)
        if file_db_id is None:
            print(file_data.get("error", None))
            return None

        print("File data sent successfully.")
        chuncks_directory = self.split_file(file_path, 1024*1024*20, str(file_db_id))
        
        if chuncks_directory is None: 
            print("Error splitting file.")
            return None
        
        print("File chunks created successfully.")
        if self.handle_uploading(chuncks_directory, file_db_id):
            print("File uploaded successfully.")
            return file_data
        else:
            print("Error uploading file.")
            return None

    
    def handle_uploading(self, chunks_dir_path: str, parent_file_id: int) -> bool:    
        chunks_list = glob.glob(f"{chunks_dir_path}/*")
        try:
            for chunk in chunks_list:
                data = {
                    "fileId": parent_file_id,
                }

                with open(chunk, 'rb') as f:
                    r = requests.post(self.api_url, data=data, files={'file': f})   
            return True
        except Exception: return False 

    def download_file(self, file_name: str) -> str | None:
        """
        Download a file from the storage.

        Parameters:
        - file_name: The exact name of the file to download.
        """
        
        file_data = self.get_file_data(file_name)
        chunks_id_list = file_data.get("chunksIds")
        output_directory = file_name.split(".")[0]
        if not Path.exists(Path(output_directory)): os.makedirs(output_directory)

        for chunk_data in chunks_id_list:
            chunk_id = chunk_data.get("chunkId")
            chunk_name = chunk_data.get("chunkName")
            r = requests.get(self.api_url + '/download', params={
                "chunkId": chunk_id
            })

            with open(os.path.join(output_directory, chunk_name), "wb") as f:
                f.write(r.content)

        return self.merge_chunks(output_directory, file_name)



