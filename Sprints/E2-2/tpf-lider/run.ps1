$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $dir
java "-Dmaven.multiModuleProjectDirectory=$dir" -cp ".mvn\wrapper\maven-wrapper.jar" org.apache.maven.wrapper.MavenWrapperMain compile exec:java
