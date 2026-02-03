# Pre-reqs : 

## Run on :
- WSL pour simulation env Linux
 - Docker Engine + Docker Compose -> Windows/WSL integration
	 - (check : )
		 - docker --version
		 - docker compose version

## Base packages (reco)
`sudo apt-get update -y`
`sudo apt-get upgrade -y`

`sudo apt-get install -y ca-certificates curl zip unzip`
# Java (openjdk-17-jdk)
`sudo apt-get install -y openjdk-17-jdk`
`java -version`

==WARNING==
If Java version diff 17 :
`sudo apt-get update -y`
`sudo apt-get install -y openjdk-17-jdk`
`sudo update-alternatives --config java` -> select java-17-openjdk-amd64
Same thing for javac : 
`sudo update-alternatives --config java` -> select java-17-openjdk-amd64

Verifs : 
`java -version`
`javac -version`

## Scala + sbt (via sdkman)

### sdk
`curl -s "https://get.sdkman.io" | bash`
`source "$HOME/.sdkman/bin/sdkman-init.sh"`
`sdk version`

### Scala (fixed version + sbt)
`sdk install scala 2.13.15`
`sdk use scala 2.13.15`
`sdk install sbt`

Verifs :
`scala -version`
`sbt --version`

## VSCode extensions
Search "Scala"
Install : 
- Scala Syntax (official)
- Scala (Metals)

### Python
`sudo apt-get install -y python3 python3-venv python3-pip`
`sudo apt-get install -y python-is-python3`
(when launch python command redirect to -> python3)

Verifs : 
`python --version`
`pip --version`


# Bonnes pratiques pour dev sous VSCode
`sbt ~compile` : compile seulement en boucle automatique, verifie que le code build correctement des que save du fichier *(arret : Ctrl+C)*
 `sbt ~test` : execution des tests unitaires -> tout ce qui est dans `src/test/scala` *(arret : Ctrl+C)*
`sbt run` : compile + execution du programme