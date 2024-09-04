pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name="kafka-streams-state-ttl-patterns"

include("common-avros")
include("common-datagen")
include("option1-iterate-all-delete")
include("option2-ktable-iterate-all-send-tombstones")
include("option3-lrucache-compact-delete")
include("option4-rocksdb-ttl-compact-delete")
include("option5-ktable-deletion-idx-range-scan")
include("option6-ktable-deletion-idx-prefix-scan")
include("option7-data-expiry-job-consumer")
include("option8-iterate-all-delete-lazy-deserializer")
