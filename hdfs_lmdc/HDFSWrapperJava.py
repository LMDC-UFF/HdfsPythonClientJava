import io
import os
from typing import Tuple, List
from PIL import Image
from py4j.java_gateway import JavaGateway

from hdfs_lmdc.HDFSWrapperBase import HDFSWrapperBase, T
from hdfs_lmdc.hdfs import RequestResult

globals()['gateway'] = None


class HadoopPythonServiceDef:

    def existsPath(self, path: str) -> bool:
        pass

    def upload(self, local_path: str, hdfs_path: str) -> bool:
        pass

    def download(self, hdfs_file_path: str, local_save_path: str = None) -> bool:
        pass

    def readAllBytes(self, path) -> bytearray:
        pass

    def mkdir(self, path) -> bool:
        pass

    def ls(self, path) -> List[str]:
        pass

import time


class HDFSWrapperJava(HDFSWrapperBase[HadoopPythonServiceDef]):

    def getClient(self) -> T:
        isReady = False
        while isReady == False:
            try:
                gateway = JavaGateway()
                print("Abrindo conexão com o hadoop no java")
                isReady = True
                # Debug
                # print(gateway.help(gateway.entry_point))
                return gateway
            except Exception as e:
                print(e)
                time.sleep(1)

    def exist_path(self, path: str) -> bool:
        client = self.getClient()
        try:
            return client.existsPath(path)
        except Exception as e:
            print(e)
        finally:
            client.close()

    def upload(self, local_path: str, hdfs_path: str) -> RequestResult:
        client = self.getClient()
        try:
            result = client.upload(local_path, hdfs_path)
            if result:
                return RequestResult.ofOk("File Uploaded")
            else:
                return RequestResult.ofError("Error upload file...! {}".format(local_path))
        except Exception as e:
            print(e)
        finally:
            client.close()

    def download(self, hdfs_file_path: str, local_save_path: str = None) -> Tuple[str, RequestResult]:
        try:
            if self.exist_path(hdfs_file_path) is False:
                return None, RequestResult.ofError("File {} not exist.".format(hdfs_file_path))

            _, local_file_name = os.path.split(hdfs_file_path)
            local_file_name, ext = os.path.splitext(local_file_name)

            local_folder_path = local_save_path
            if local_save_path is None:
                local_folder_path = os.path.join((os.sep + "tmp"), local_file_name)

            local_file_path = os.path.join(local_folder_path, local_file_name + ext)
            os.makedirs(local_folder_path, exist_ok=True)
            client = self.getClient()
            try:
                client.download(hdfs_file_path, local_file_path)
            except Exception as e:
                print(e)
            finally:
                client.close()

            return local_file_path, RequestResult.ofOk("File downloaded")
        except:
            return None, RequestResult.ofError("Download File {} failure.".format(hdfs_file_path));

    def read_txt(self, hdfs_text_path: str) -> Tuple[str, RequestResult]:
        try:
            if self.exist_path(hdfs_text_path) is False:
                return (
                    None,
                    RequestResult.ofError(
                        "File {} not exist.".format(hdfs_text_path)
                    ),
                )
            client = self.getClient()
            try:
                return (
                    client.readAllBytes(hdfs_text_path).decode("utf-8"),
                    RequestResult.ofOk(
                        "File {} read successfully.".format(hdfs_text_path)
                    ),
                )
            except Exception as e:
                print(e)
            finally:
                client.close()

        except:
            pass

        return (
            None,
            RequestResult.ofError(
                "Could not open file {}.".format(hdfs_text_path)
            ),
        )

    def read_image(self, hdfs_image_path: str):
        try:
            if self.exist_path(hdfs_image_path) is False:
                return (
                    None,
                    RequestResult.ofError(
                        "File {} not exist.".format(hdfs_image_path)
                    ),
                )
            import time
            start = time.time()
            client = self.getClient()
            content = None
            try:
                content = client.readAllBytes(hdfs_image_path)
            except Exception as e:
                print(e)
            finally:
                client.close()

            end = time.time()
            print(end - start)
            img = Image.open(io.BytesIO(content))
            return (
                img.convert('RGB'),
                RequestResult.ofOk(
                    "File {} readed and converted to RGB.".format(hdfs_image_path)
                ),
            )
        except Exception as e:
            print(e)
            pass

        return (
            None,
            RequestResult.ofError(
                "Could not open file {}.".format(hdfs_image_path)
            ),
        )

    def mkdir(self, path: str) -> bool:
        client = self.getClient()
        try:
            return client.mkdir(path)
        except Exception as e:
            print(e)
        finally:
            client.close()

    def ls(self, path) -> List[str]:
        client = self.getClient()
        try:
            resultQuery = client.ls(path)
            result = [path for path in resultQuery]
            return result
        except Exception as e:
            print(e)
        finally:
            client.close()

    def is_file(self, path):
        client = self.getClient()
        try:
            result = client.isFile(path)
            return result
        except Exception as e:
            print(e)
        finally:
            client.close()
        return None

    def is_dir(self, path):
        client = self.getClient()
        try:
            result = client.isDirectory(path)
            return result
        except Exception as e:
            print(e)
        finally:
            client.close()
        return None

    def walk(self, path):
        client = self.getClient()
        try:
            resultQuery = client.pathInfo(path)
            files = [path for path in resultQuery.getFiles()]
            dirs = [path for path in resultQuery.getFolders()]
            yield path, dirs, files
            for dir in dirs:
                yield from self.walk(os.path.join(path, dir))
        except Exception as e:
            print(e)
        finally:
            client.close()
