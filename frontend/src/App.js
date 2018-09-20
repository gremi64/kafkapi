import React, { Component } from 'react';
import TopicOffsets from './TopicOffsets';
import TopicOffsetsForm from './TopicOffsetsForm';
import { Icon, Header, Container } from 'semantic-ui-react';

class App extends Component {

    constructor(props) {
        super(props);
        this.state = {
            topicForm: "he_dev_executableOperation_priv_v1",
            groupForm: "he_dev_u5_executableOperation",
            result: {
                topic: "test",
                group: "",
                partitionOffsetResult: []
            },
            brokers: [],
            securities: []
        };
        this.handleClick = this.handleClick.bind(this);
    }

    componentDidMount() {
        this.offsetsResult();
        this.getConsumerConfigs();
        //setInterval(this.offsetsResult, 10000);
    }

    offsetsResult = () => {
        fetch('/offsets/' + this.state.topicForm + '?group=' + this.state.groupForm)
            .then(response => {
                return response.json()
            })
            .then(message => {
                if (message) {
                    this.setState({ result: message });
                }
            });
    };

    getConsumerConfigs() {
        fetch('/config/consumers')
        .then(response => {
            return response.json()
        })
        .then(message => {
            if (message) {
                var brokers = [];
                message.brokers.forEach(function(broker) {
                    brokers.push({ key: broker.first, text: broker.second, value: broker.second });
                });

                var securities = [];
                message.securityOptions.forEach(function(security) {
                    securities.push({ key: security.first, text: security.second, value: security.second });
                });

                this.setState({ brokers: brokers });
                this.setState({ securities: securities });
            }
        });
    };

    handleClick(event) {
        this.setState({
            topicForm: event.target.myTopic.value,
            groupForm: event.target.myGroup.value
        }, function () {
            this.offsetsResult();
        });
    }

    render() {
        return (
            <div className="App">
                <Container textAlign='center'>
                    <Header as='h2' icon textAlign='center'>
                        <Icon name='blind' circular />
                    </Header>
                    <TopicOffsetsForm 
                        handleClick={ this.handleClick }
                        topicForm={ this.state.topicForm }
                        groupForm={ this.state.groupForm }
                        brokers={ this.state.brokers }
                        securities={ this.state.securities } />
                    <br/>
                    <TopicOffsets 
                        topic={ this.state.result.topic }
                        group={ this.state.result.group }
                        partitionOffsetResult={ this.state.result.partitionOffsetResult } />
                </Container>
            </div>
        );
    }
}

export default App;
