package br.uff.lmdc.HadoopPythonServer;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("hadoop")
public class HadoopConfigurationProperties {
  @Getter
  @Setter
  @NestedConfigurationProperty
  HadoopHDFSConfigurationProperties hdfs = new HadoopHDFSConfigurationProperties();

  @Getter
  @Setter
  @ToString
  public static class HadoopHDFSConfigurationProperties {
    //Padrão
    private String username = "hdfs";
    //private String username = "root/labesi.lmdc.uff.br@LMDC";
    private boolean disable = false;
    private Byte replicationFactor = 1;
    private Boolean forceUseDataNodeHostname = true;

    //Se usar aquivo de xml
    private boolean useXmlResources = false;
    private String coreSitePath = null;
    private String hdfsSitePath = null;

    //Se usar aquivo de propriedades
    private String hosts = null;
    private boolean useWebHdfs = false;

    //Se usar o kerberos
    private boolean useKerberos = false;
    private String userKeyTabPath = null;

  }

}
