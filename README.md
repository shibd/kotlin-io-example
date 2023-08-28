# kotlin-io-example
Use Kotlin develop pulsar connector example

1. maven clean install 
2. [Start pulsar broker](https://pulsar.apache.org/docs/3.1.x/getting-started-docker/)
3. Start this connector by localrun
```sh
./pulsar-admin sinks localrun -a {Your project path}/kotlin-io-example/target/kotlin-io-example-0.0.1.nar --name test-io-kotlin --inputs test-io
```
4. You will see on console:
```log
HELLO
```