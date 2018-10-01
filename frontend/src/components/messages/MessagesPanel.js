import React, { Component } from 'react';
import { Container, Divider } from 'semantic-ui-react';

import MessagesForm from './MessagesForm';
import MessagesTables from './MessagesTables';

class MessagesPanel extends Component {
  constructor(props) {
    super(props);
    this.state = {
      messages: null,
      brokers: [],
      securities: []
    };
  };

  render() {
    return (
      <Container>
        <MessagesForm 
          brokers={ this.state.brokers } 
          securities={ this.state.securities } 
          onFormSubmit={ this.onFormSubmit } />
        <Divider hidden/>
        <MessagesTables 
          messages={ this.state.messages } 
          loadingOffset={ this.state.loadingOffset } />
      </Container>
    );
  };

  componentDidMount() {
    console.log('Mounted message panel');
    this.getConsumerConfigs();
  };

  onFormSubmit = (selectedTopic, selectedGroup, selectedBrokers, selectedSecurity) => {
    this.setState({
      selectedTopic: selectedTopic,
      selectedGroup: selectedGroup,
      selectedBrokers: selectedBrokers,
      selectedSecurity: selectedSecurity
    }, function () {
      this.getMessages();
    });
  };

  getMessages = () => {
    this.setState({ loadingOffset: true }, function() {
      var uri = '/messages/' + this.state.selectedTopic + '?group=' + this.state.selectedGroup;
      console.log('Calling API on ' + uri);
      fetch(uri)
      .then(response => {
          return response.json()
      })
      .then(message => {
          if (message) {
            this.setState({ 
              messages: message,
              loadingOffset: false
            });
          }
      });
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
};

export default MessagesPanel;