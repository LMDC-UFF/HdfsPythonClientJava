package br.uff.lmdc.HadoopPythonServer;


import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This is the Hadoop HDFS Service which is responsible for all activity involving the HDFS.
 * This and DataLakeService take care of the physical layer of the Data Lake application.
 * The DataLakeService takes care of handling the files names and directories using hash function, and
 * this class takes care of the actual management of the files in HDFS.
 */
@Log4j2
@Service
public class HadoopHDFSService {

  private HadoopConfigurationProperties hadoopConfiguration;

  public HadoopHDFSService(HadoopConfigurationProperties hadoopConfiguration) {
    this.hadoopConfiguration = hadoopConfiguration;
  }

  private Configuration currentConfiguration;
  private FileSystem fs;

  @PostConstruct
  public void onInit() throws IOException, InterruptedException {
    if (!hadoopConfiguration.getHdfs().isDisable()) {
      log.info(hadoopConfiguration.getHdfs());
      this.currentConfiguration = this.buildHDFSConfiguration();
      if (this.hadoopConfiguration.getHdfs().isUseKerberos()) {
        currentConfiguration.set("hadoop.security.authentication", "kerberos");
      }
      //desabilitar essa configuracao trás o erro de block missing
      currentConfiguration.set("dfs.client.use.datanode.hostname", this.hadoopConfiguration.getHdfs().getForceUseDataNodeHostname().toString());
      fs = configurationToFS(this.currentConfiguration);
      log.info("Hadoop getCanonicalServiceName" +  fs.getCanonicalServiceName());
      log.info("Hadoop getUri" +  fs.getUri());
    }
  }

  private Configuration buildHDFSConfiguration() throws IOException {
    if (hadoopConfiguration.getHdfs().isUseXmlResources()) {
      return buildXmlResourcesHDFSConfiguration();
    } else {
      return buildSimpleHDFSConfiguration();
    }
  }

  FileSystem configurationToFS(Configuration configuration) throws IOException, InterruptedException {
    UserGroupInformation ugi;
    if (this.hadoopConfiguration.getHdfs().isUseKerberos()) {
      UserGroupInformation.setConfiguration(configuration);
      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(this.hadoopConfiguration.getHdfs().getUsername(), Paths.get(this.hadoopConfiguration.getHdfs().getUserKeyTabPath()).toString());
    } else {
      ugi = UserGroupInformation.createRemoteUser(this.hadoopConfiguration.getHdfs().getUsername());
    }
    return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(configuration));
  }

  Configuration buildXmlResourcesHDFSConfiguration() {
    val conf = new Configuration();
    conf.addResource(new Path(this.hadoopConfiguration.getHdfs().getCoreSitePath()));
    conf.addResource(new Path(this.hadoopConfiguration.getHdfs().getHdfsSitePath()));
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.set("fs.default.name", conf.get("fs.defaultFS"));
    return conf;
  }

  Configuration buildSimpleHDFSConfiguration() {
    Configuration conf = new Configuration();
    String clusterNameNode = "NameNodeCluster";

    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.set("fs.defaultFS", "hdfs://" + clusterNameNode);
    conf.set("fs.default.name", conf.get("fs.defaultFS"));
    conf.set("dfs.nameservices", clusterNameNode);

    List<String> hdfsHosts = Arrays.asList(this.hadoopConfiguration.getHdfs().getHosts().split(Pattern.quote(",")));
    conf.set("dfs.ha.namenodes." + clusterNameNode, IntStream.range(0, hdfsHosts.size()).mapToObj(i -> "nn" + i).collect(Collectors.joining(",")));

    int index = 0;
    for (String host : hdfsHosts) {
      conf.set("dfs.namenode.rpc-address." + clusterNameNode + ".nn" + (index++), host);
    }

    conf.set("dfs.client.failover.proxy.provider." + clusterNameNode, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    conf.set("dfs.replication", this.hadoopConfiguration.getHdfs().getReplicationFactor().toString());

    return conf;
  }

  /**
   * Return an array of FileStatus (hdfs files) by a given hdfs path.
   *
   * @param path the hdfs path.
   * @return The list of FileStatus.
   * @throws IOException
   */
  public FileStatus[] showDirectory(String path) throws IOException {
    log.debug("showDirectory() called for {}", path);
    FileStatus[] fileStatus = fs.listStatus(new Path(path));
    return fileStatus;
  }

  /**
   * Check if a hdfs path currently exists or not
   *
   * @param pathStr the hdfs path
   * @return true if it exists, false if it doesn't
   * @throws IOException
   */
  public boolean existsPath(String pathStr) throws IOException {
    Path path = new Path(pathStr);
    boolean result = fs.exists(path);
    log.debug("existsPath() called for '{}' Returned '{}'", pathStr, result);
    return result;
  }

  public boolean mkdir(java.nio.file.Path path) throws IOException, HadoopException {
    return makeDirectory(path.getFileName().toString(), path.getParent().toString());
  }

  @Autowired
  MessageSource messageSource;

  /**
   * Create a new directory in hdfs, if there isn't one of the same name yet.
   *
   * @param name name of the directory
   * @param path path of the directory
   * @return
   * @throws IOException
   */
  public boolean makeDirectory(String name, String path) throws IOException, HadoopException {
    log.debug("makeDirectory() called. Path='{}'; Name='{}'", path, name);
    if (!path.equals("")) {
      if (!existsPath(path)) {
        String aux[] = new String[1];
        aux[0] = path;
        String msg = messageSource.getMessage("Hadoop.PathInv", aux, LocaleContextHolder.getLocale());
        log.error("Não foi possível criar diretório. O caminho não existe: '" + path + "'");
        throw new HadoopException(msg);
//        throw new HadoopException("Não foi possível criar diretório. O caminho não existe: '" + path + "'");
      }
    }
    Path newDirectoryName = new Path(new Path(path), name);
    if (!fs.exists(newDirectoryName)) {
      fs.mkdirs(newDirectoryName);
      log.debug("Diretório {} foi criado em {} ", name, path);
      return true;
    } else {
      log.debug("Diretório não foi criado; ele já existe: {}/{}", path, name);
      return false;
    }
  }

  /**
   * Write to hdfs in a given path from an inputStream source data.
   *
   * @param sourceData inputStream of the local file.
   * @param outputPath the path in hdfs.
   * @param fileName   name of the file to be written to hdfs.
   * @param append     whether to append the file or not.
   * @throws IOException
   */
  public void writeFile(InputStream sourceData, String outputPath, String fileName, boolean append) throws IOException {
    try (FSDataOutputStream outputStream = getOutputStreamForWrite(outputPath, fileName, append)) {
      IOUtils.copy(sourceData, outputStream.getWrappedStream());
      log.debug("Arquivo {} escrito em {} ", fileName, outputPath);
    }
  }

  /**
   * Return a OutputStream for a path on hdfs to write files to.
   *
   * @param outputPath the destination hdfs path.
   * @param fileName   the name of the file
   * @param append     whether to append to the file or not, in case it exists.
   * @return the OutputStream
   * @throws IOException
   */
  public FSDataOutputStream getOutputStreamForWrite(String outputPath, String fileName, boolean append) throws IOException {
    log.debug("writeFile() called.");
    Path hdfswritepath = new Path(outputPath, fileName);
    if (fs.exists(hdfswritepath)) {
      if (append) {
        return fs.append(hdfswritepath);
      }
    }
    return fs.create(hdfswritepath, true);
  }

  /**
   * Return an inputStream for a file by a path from hdfs.
   *
   * @param filePath the path of the hdfs file
   * @return the inputStream
   * @throws IOException
   */
  public InputStream readFile(String filePath) throws IOException {
    log.debug("readFile() called.");
    Path hdfsreadpath = new Path(filePath);
    return fs.open(hdfsreadpath);
  }

  public boolean deleteFile(java.nio.file.Path filePath, boolean isRecursive) throws IOException, HadoopException {
    return this.deleteFile(filePath.toString(), isRecursive);
  }

  /**
   * Remove a file/folder from hdfs by a path
   *
   * @param filePath    the path of the folder/file
   * @param isRecursive to whether or not remove contents recursively or not.
   * @return true if the file was successfully deleted, false if not
   * @throws IOException
   * @throws HadoopException
   */
  public boolean deleteFile(String filePath, boolean isRecursive) throws IOException, HadoopException {
    log.debug("deleteFile() called.");
    Path path = new Path(filePath);
    if (fs.exists(path)) {
      if (fs.delete(path, isRecursive)) {
        log.debug("Arquivo deletado.");
        return true;
      }
      log.debug("Houve um problema para remover o arquivo.");
      return false;
    } else {
      log.debug("Não foi possível remover {}. O arquivo não existe.", filePath);
      String aux[] = new String[1];
      aux[0] = filePath;
      String msg = messageSource.getMessage("FileNotEx", aux, LocaleContextHolder.getLocale());
      throw new HadoopException(msg, new FileNotFoundException(msg));
//      throw new HadoopException("O arquivo '" + filePath + "' não existe", new FileNotFoundException("O arquivo '" + filePath + "' não existe."));
    }
  }

  /**
   * Move a file inside of the hdfs
   *
   * @param src The source path of the file in hdfs
   * @param dst The destination path of the file in hdfs
   * @return
   * @throws IOException
   * @throws HadoopException
   */
  public boolean moveFile(String src, String dst) throws IOException, HadoopException {
    log.debug("moveFile() chamado. De {} para {}.", src, dst);
    Path srcPath = new Path(src);
    Path dstPath = new Path(dst);
    if (fs.exists(srcPath)) {
      if (fs.rename(srcPath, dstPath)) {
        log.debug("Arquivo '{}' renomeado para '{}'", src, dst);
        return true;
      }
      log.debug("Houve um problema para mover o arquivo.");
      return false;
    } else {
      log.debug("Não foi possível renomear {}. O arquivo/diretório não existe", src);
      String aux[] = new String[1];
      aux[0] = src;
      String msg = messageSource.getMessage("FileNotEx", aux, LocaleContextHolder.getLocale());
      throw new HadoopException(msg, new FileNotFoundException(msg));
    }
  }

  /**
   * Return a {@link FileStatus} of directory by path. The FileStatus can be used to list info of a file.
   *
   * @param filePath
   * @return FileStatus
   * @throws IOException
   */
  public FileStatus fileInfo(String filePath) throws IOException {
    log.debug("fileInfo() chamado para {}", filePath);
    Path path = new Path(filePath);
    FileStatus fileStatus = fs.getFileStatus(path);
    return fileStatus;
  }

  /**
   * Save ExtendedAttributes in Hdfs Area
   *
   * @param filePath The source path of the file in hdfs
   * @param name     The configuration of attributes
   * @param data     Attributes in bytes
   * @throws IOException If not possible to write  the Extended Attribute
   */
  public void setExtendedAttributes(String filePath, String name, byte[] data) throws IOException {
    try {
      fs.setXAttr(new Path(filePath), name, data);
    } catch (IOException e) {
      log.error("Não foi possível gravar o atributo estendido: '" + name + "'.", e);
      throw e;
    }
  }

  public void setExtendedAttributes(String filePath, String name, String data) throws IOException {
    this.setExtendedAttributes(filePath, name, data.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Get Extended Attribute in HDFS
   *
   * @param filePath The source path of the file in hdfs
   * @param name     The configuration of attributes
   * @return bytes[]
   * @throws IOException If not possible to read the Extended Attribute
   */
  public byte[] getExtendedAttribute(String filePath, String name) throws IOException {
    try {
      return fs.getXAttr(new Path(filePath), name);
    } catch (IOException e) {
      log.debug("Não foi possível resgatar o atributo estendido '" + name + "'. O caminho ou nome não existe", e);
      if (e.getMessage().startsWith("At least one of the attributes provided was not found.")) {
        return null;
      }
      throw e;
    }
  }

  public Optional<String> getExtendedAttributeString(String filePath, String name) throws IOException {
    byte[] data = this.getExtendedAttribute(filePath, name);
    return Optional.ofNullable(data != null ? new String(data, StandardCharsets.UTF_8) : null);
  }

  public void removeExtendedAttribute(String path, String name) throws IOException {
    try {
      fs.removeXAttr(new Path(path), name);
    } catch (IOException e) {
      log.error("Não foi possível remover o atributo estendido:'" + name + "'", e);
      throw e;
    }
  }

}
