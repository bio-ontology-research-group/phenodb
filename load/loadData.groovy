@GrabResolver(name='virtuoso-github', root='https://raw.githubusercontent.com/kops/jena-virtuoso-example/maven/repository', m2Compatible='true')
@Grapes([
    @Grab(group='org.apache.jena', module='apache-jena-libs', version='2.12.0'),
    @Grab(group='com.openlink.virtuoso', module='virtjdbc4-1', version='3.74'),
    @Grab(group='com.openlink.virtuoso', module='virt_jena2', version='1.12'),
    @Grab(group='org.slf4j', module='slf4j-api', version='1.7.28'),
    @Grab(group='log4j', module='log4j', version='1.2.16'),
])


import virtuoso.jena.driver.VirtModel;

import com.hp.hpl.jena.rdf.model.*; 

import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.io.InputStream;
import java.util.Properties;

import virtuoso.jena.driver.*;

Model model = null;
InputStream input = null;
try {
    scriptDir = new File(getClass().protectionDomain.codeSource.location.path).parent
    input = new FileInputStream(scriptDir + "/config.properties")
    Properties prop = new Properties();
    prop.load(input);


    virtuosoUrl = prop.get("virtuoso.url")
    virtuosoUser = prop.get("virtuoso.user")
    virtuosoPwd = prop.get("virtuoso.pwd")
    directory = new File(scriptDir + "/data");
    // model = ModelFactory.createDefaultModel();

    println("Connecting to virtuoso:" + virtuosoUrl + "|" + virtuosoUser + "|" + virtuosoPwd)
    model = VirtModel.openDatabaseModel("http://phenodb.phenomebrowser.net", "jdbc:virtuoso://" + virtuosoUrl, virtuosoUser, virtuosoPwd);

    model.removeAll();
    println("loading data started Starting ...")
    final def thisModel = model;
    Files.walkFileTree(Paths.get(directory.toURI()), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile( final Path file, final BasicFileAttributes attrs) throws IOException {
            if (file.getFileName().toString().endsWith(".owl") || file.getFileName().toString().endsWith(".rdf")) {
                println("loading rdf data in file:" + file.getFileName())
                thisModel.read( file.toUri().toString() ,"RDF/XML");
            }
            return super.visitFile(file, attrs);
        }
    });
    println("Data Loaded successfully:" + thisModel.size())
} catch (Exception e) {
  e.printStackTrace();
} finally {
    if (model != null) 
        model.close();
    if (input != null) 
        input.close();
}
