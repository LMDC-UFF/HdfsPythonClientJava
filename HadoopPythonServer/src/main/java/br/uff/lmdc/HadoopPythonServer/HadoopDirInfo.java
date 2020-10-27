package br.uff.lmdc.HadoopPythonServer;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;

@Getter
@Setter
public class HadoopDirInfo {

    String path;
    String[] folders;
    String[] files;

    public HadoopDirInfo(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "HadoopDirInfo{" +
                "path='" + path + '\'' +
                ", folders=" + Arrays.toString(folders) +
                ", files=" + Arrays.toString(files) +
                '}';
    }
}
