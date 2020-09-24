package br.uff.lmdc.HadoopPythonServer;

public class HadoopException extends Throwable{
    public HadoopException() {
    }

    public HadoopException(String s) {
        super(s);
    }

    public HadoopException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public HadoopException(Throwable throwable) {
        super(throwable);
    }

    public HadoopException(String s, Throwable throwable, boolean b, boolean b1) {
        super(s, throwable, b, b1);
    }
}
