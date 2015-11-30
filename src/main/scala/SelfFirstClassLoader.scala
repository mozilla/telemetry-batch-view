package telemetry

import java.io.File
import java.lang.ClassLoader
import java.net.{URL, URLClassLoader}

class SelfFirstClassLoader(classpath: Array[URL], parent: ClassLoader) extends URLClassLoader(classpath, parent) {
  @throws(classOf[ClassNotFoundException])
  override final def loadClass(className: String, resolve: Boolean): Class[_] = {
    val loaded = findLoadedClass(className)

    val found =
      if(loaded == null) {
        try {
          findClass(className)
        } catch {
          case _: ClassNotFoundException =>
            super.loadClass(className, false)
        }
      } else loaded

    if (resolve)
      resolveClass(found)

    found
  }
}

object SelfFirstClassLoader {
  private def getListOfFiles(dir: File): Array[URL] = {
    val these = dir.listFiles
    these.map(_.toURL) ++ these.filter(_.isDirectory).flatMap(getListOfFiles)
  }

  def apply(directory: String): SelfFirstClassLoader = {
    val files = getListOfFiles(new File(directory))
    new SelfFirstClassLoader(files, this.getClass.getClassLoader)
  }
}
