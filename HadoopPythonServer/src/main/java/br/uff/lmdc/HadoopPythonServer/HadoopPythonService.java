package br.uff.lmdc.HadoopPythonServer;

import io.netty.handler.codec.http.multipart.FileUpload;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Paths;

@Log4j2
@Service
public class HadoopPythonService {

    private HadoopHDFSService hadoopHDFSService;

    public HadoopPythonService(HadoopHDFSService hadoopHDFSService) {
        this.hadoopHDFSService = hadoopHDFSService;
    }

    public boolean existsPath(String path) throws IOException {
        log.info("existsPath: {}", path);
        return this.hadoopHDFSService.existsPath(path);
    }

    public boolean upload(String local_path, String hdfs_path) {
        log.info("upload: {} -> {}", local_path, hdfs_path);
        val file = new File(local_path);

        try (val input = new FileInputStream(file)) {
            hadoopHDFSService.writeFile(input, hdfs_path, file.getName(), false);
            return true;
        } catch (Exception e) {
            log.error(e);
        }
        return false;
    }

    public byte[] readAllBytes(String path) {
        log.info("readAllBytes {}", path);
        val start = System.currentTimeMillis();
        try (val input = hadoopHDFSService.readFile(path)) {
            byte[] bytes = IOUtils.toByteArray(input);
            val end = System.currentTimeMillis();
            System.out.println(">>>>>>>>>>>>>>>>>. " + (end - start));
            return bytes;
        } catch (IOException e) {
            log.error(e);
        }
        return null;
    }

    public boolean download(String hdfs_file_path, String local_save_path) {
        log.info("download: {} -> {}", hdfs_file_path, local_save_path);
        try (FileOutputStream fileOutputStream = new FileOutputStream(local_save_path)) {
            val input = hadoopHDFSService.readFile(hdfs_file_path);
            IOUtils.copy(input, fileOutputStream);
            IOUtils.closeQuietly(input);
            return true;
        } catch (Exception e) {
            log.error(e);
        }
        return false;
    }
}
