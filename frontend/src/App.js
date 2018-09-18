import React, { Component } from 'react';
import TopicOffsets from './TopicOffsets';
import logo from './logo.svg';
import './App.css';

class App extends Component {

    state = {};

    componentDidMount() {
        setInterval(this.hello, 250);
    }

    hello = () => {
        return "hahahaha";
        // fetch('/api/hello')
        //     .then(response => response.text())
        //     .then(message => {
        //         this.setState({message: message});
        //     });
    };

    render() {
        return (
            <div className="App">
                <TopicOffsets/>
            </div>
        );
    }
}

export default App;
