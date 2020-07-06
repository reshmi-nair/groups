if [ "$1" == "skip" ]; then
 mvn clean install -DskipTests
else
 mvn clean install
fi

cd service
if [ "$2" == "copy" ]; then
  mvn play2:dist
  cp target/*.zip ../
  echo "Finished copying..."
else
  echo "running..."
  mvn play2:run
fi
cd ..