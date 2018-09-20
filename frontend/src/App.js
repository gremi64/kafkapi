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
            }
        };
        this.handleClick = this.handleClick.bind(this)
        this.offsetsResult();
    }

    componentDidMount() {
        setInterval(this.offsetsResult, 10000);
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
                    <TopicOffsetsForm handleClick={this.handleClick} topicForm={this.state.topicForm} groupForm={this.state.groupForm} />
                    <br/>
                    <TopicOffsets topic={this.state.result.topic} group={this.state.result.group} partitionOffsetResult={this.state.result.partitionOffsetResult} />
                </Container>
            </div>
        );
    }
}

export default App;
