# Kafkapi

This is a all-in-one tool to do some stuff with Kafka. 

It has been created and is developed with :heart: by two developers ([gremi64](https://github.com/gremi64) and [Crazymeal](https://github.com/CrazyMeal)) during their lunch time :pizza: and off-work time :house:

We are using [React](https://reactjs.org/) ![react logo](https://cdn.auth0.com/blog/react-js/react.png | width=50) for the frontend and [Spring Boot](https://spring.io/projects/spring-boot) with [Kotlin](https://kotlinlang.org/) for the backend

## Features

### Usable with frontend

- Show offsets of a given topic with a given group
- Show messages of a given topic (still working on it :hammer: )

### Only with backend

- Show offsets of a given topic
- Show offsets of a given partition of a topic
- Show messages of a given topic
- Show messages of a given partition of topic
- Show available configs: brokers and security (need improvments for dynamic config :hammer: )

## Compatibility
The tool is developed using versions of Kafka you can find in [docker-compose file](kafkapi/blob/master/src/main/resources/docker-compose.yml)

