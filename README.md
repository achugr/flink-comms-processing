<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
  * [Motivation](#motivation)
  * [Use cases](#built-with)
    * [Events archiving](#events-archiving)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Future work](#future-work)
* [Results](#results)
* [Main outcomes](#main-outcomes)
* [Contributing](#contributing)
* [License](#license)



<!-- ABOUT THE PROJECT -->
## About The Project

### Motivation
Main motivation is to try [Apache Flink](https://flink.apache.org/) in the context of [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) 
task and to understand system abilities in general.

### Use cases

#### Events archiving
Consume events and prepare personal archives for long-term storing purposes and effective extraction.
  * Archives are organized by time range (weekly archives, for example)
  * Archives have main person (i.e. all in/out communication of person for data range)
  * Events should be searchable, so that we know what are participants and dates of the events we are potentially interested
Having this we could effectively extract all communication of concrete employee for concrete dates from deep archives
like [AWS Glacier](https://aws.amazon.com/glacier/).

<!-- GETTING STARTED -->
## Getting Started

### Prerequisites

This sample requires 
* java (checked with 1.8)
* docker, docker compose
* gradle

### Installation

1. Clone the repo
2. Download [Enron email](https://www.cs.cmu.edu/~./enron/) dataset
3. Open the project in IDE like IntellijIdea
4. Run containers `sudo docker-compose up -d` from `docker` folder. This would start
    * [Minio](https://min.io/) - for result archives, flink checkpoints and big payloads storing. S3-compatible
    * [Elasticsearch](https://www.elastic.co/elasticsearch) - for data index
    * [DejaVu](https://github.com/appbaseio/dejavu) - viewer for elasticsearch
5. Run CommsEtlJob class with java option `-DSOURCE_FOLDER=./enron` to target the directory with extracted Enron dataset
6. Check flink dashboard at http://localhost:8081/
7. Check the built index at http://localhost:1359/?appname=data&url=http://localhost:9200&mode=edit
8. Check prepared archives at http://localhost:9000/minio/archived/

### Future work

* Add tests...Basically implemented logic is very simple, but would be great to have at least integration test 
to check overall job correctness. 
* Introduce simple dataset generator and check with more events in cluster mode
* Add more event types (with events hierarchy and all these things) 
* Handle attachments
* Make some heavy parts of payload (like heavy attachments) lazy - fetch them from object storage only by request 
* Add some analytics pipelines (try detect anomalies, build a social graph etc - dataset doesn't contain really significant emails,
 but still, the idea why this dataset is public is clear [Enron scandal](https://www.investopedia.com/updates/enron-scandal-summary/))

### Results
Enron dataset (about 1.7G unarchived) can be processed locally in less than 10 minutes. It doesn't say much though )
![Archives bucket folder structure](https://user-images.githubusercontent.com/853775/96398094-02d34200-1199-11eb-9642-4372fc190982.png)
![Indexed contents](https://user-images.githubusercontent.com/853775/96398097-05359c00-1199-11eb-937b-fbc3b65dbab5.png)

### Main outcomes
Flink looks pretty good for such type of tasks, however the fact that events are of arbitrary size could bring complexities, but 
approach like reference based messaging solves this problem.
Even complex and task-specific logic could be introduced (calling different storages during processing, creating many data flows with own sinks etc), 
but one day it might become a source of huge performance and logical issues. On the other hand limitations of the paradigm would force you to
think twice before introducing complex, potentially unnecessary logic, or avoid bringing to the pipeline things that can be done outside 
of the pipeline scope.

<!-- CONTRIBUTING -->
## Contributing

Contribution is welcome: starting from refactoring and improvements and ending with new jobs for analysis of archived events.

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.
