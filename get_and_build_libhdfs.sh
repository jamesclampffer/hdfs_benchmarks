rm -rf hadoop_source
mkdir hadoop_source

cd hadoop_source
git clone https://github.com/apache/hadoop.git .
git checkout HDFS-8707
mvn install -Pnative -Pdist -Dtar -DskipTests -q -Dmaven.javadoc.skip=true -Djava.awt.headless=true
mvn install -pl :hadoop-hdfs-native-client -Pnative -Pdist -Dtar -Dmaven.javadoc.skip=true
LIB_PATH=realpath `find -iname libhdfspp.*so`
cp $LIB_PATH ../
