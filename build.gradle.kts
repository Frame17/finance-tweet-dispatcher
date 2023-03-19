plugins {
    kotlin("jvm") version "1.8.0"
    application
}

group = "frame"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx.spark:kotlin-spark-api_3.3.1_2.13:1.2.3")
    implementation("org.apache.spark:spark-mllib_2.13:3.3.2")

    compileOnly("org.apache.spark:spark-sql_2.13:3.3.2")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("MainKt")
}