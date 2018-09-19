import React, { Component } from 'react';
import TopicOffsets from './TopicOffsets';
import TopicOffsetsForm from './TopicOffsetsForm';
import './App.css';

class App extends Component {

    constructor(props) {
        super(props);
        this.state = {
            buttonTitle: "Go !",
            topicForm: null,
            groupForm: null,
            result: {
                topic: "test",
                group: "",
                partitionOffsetResult: []
            }
        };
        this.offsetsResult();
    }

    componentDidMount() {
        setInterval(this.offsetsResult, 10000);
    }

    offsetsResult = () => {
        fetch('/offsets/he_dev_executableOperation_priv_v1?group=he_dev_u5_executableOperation')
            .then(response => {
                return response.json()
            })
            .then(message => {
                if (message) {
                    this.setState({ result: message });
                }
            });
    };

    render() {
        return (
            <div className="App">
                <TopicOffsetsForm buttonTitle={this.state.buttonTitle} topicForm={this.state.topicForm} groupForm={this.state.groupForm}/>
                <TopicOffsets topic={this.state.result.topic} group={this.state.result.group} partitionOffsetResult={this.state.result.partitionOffsetResult} />
            </div>
        );
    }
}

export default App;
