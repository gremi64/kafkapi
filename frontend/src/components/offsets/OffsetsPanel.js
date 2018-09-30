import React, { Component } from 'react';
import { Icon, Header, Container, Divider } from 'semantic-ui-react';

import TopicOffsets from './TopicOffsets';
import TopicOffsetsForm from './TopicOffsetsForm';

class OffsetsPanel extends Component {

  constructor(props) {
    super(props);
    this.state = {
        topicForm: "test-topic",
        groupForm: "myGroup",
        result: {
            topic: "test",
            group: "",
            partitionOffsetResult: []
        },
        brokers: [],
        securities: [],
        loadingOffset: false,
    };
  };

  render() {
    return (
      <Container textAlign='center'>
        <Divider hidden />
        <Header as='h2' icon textAlign='center'>
          <Icon name='blind' circular />
        </Header>
        <TopicOffsetsForm 
            onClickForm={ this.onClickForm }
            topicForm={ this.state.topicForm }
            groupForm={ this.state.groupForm }
            brokers={ this.state.brokers }
            securities={ this.state.securities } />
        <br/>
        <TopicOffsets 
            topic={ this.state.result.topic }
            group={ this.state.result.group }
            partitionOffsetResult={ this.state.result.partitionOffsetResult } 
            loadingOffset= { this.state.loadingOffset }/>
      </Container>
    );
  };

  componentDidMount() {
    this.getConsumerConfigs();
  };

  getOffsetsResult = () => {
    console.log('Calling offset API state {"topic": "' + this.state.topicForm + '", "group": "' + this.state.groupForm + '", "brokers": ' + this.state.brokersForm + '", "security": "' + this.state.securityForm + '"}');
    
    if (this.state.topicForm) {
        var uri = '/offsets/' + this.state.topicForm;
        this.setState({ loadingOffset: true });
        
        if (this.state.groupForm || this.state.brokersForm || this.state.securityForm) {
            uri += '?';
            var thereWasPreviousRequestParam = false;
            if (this.state.groupForm) {
                uri += 'group=' + this.state.groupForm;
                thereWasPreviousRequestParam = true;
            }
            if (this.state.groupForm) {
                if (thereWasPreviousRequestParam) {
                    uri += '&';
                } 
                uri += 'brokers=' + this.state.brokersForm;
            }
            if (this.state.groupForm) {
                if (thereWasPreviousRequestParam) {
                    uri += '&';
                } 
                uri += 'security=' + this.state.securityForm;
            }

            console.log('Fetch using uri: ' + uri);
            fetch(uri)
            .then(response => {
                return response.json()
            })
            .then(message => {
                if (message) {
                    this.setState({ result: message, loadingOffset: false });
                }
            });
        }
    }        
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

  onClickForm = (topicForm, groupForm, brokersForm, securityForm) => {
      console.log('Submitted form with {"topic": "' + topicForm + '", "group": "' + groupForm + '", "brokers": ' + brokersForm + '", "security": "' + securityForm + '"}');
      this.setState({
          topicForm: topicForm,
          groupForm: groupForm,
          brokersForm: brokersForm,
          securityForm: securityForm,
      }, function () {
          this.getOffsetsResult();
      });
  };
};

export default OffsetsPanel;
