import React, { Component } from 'react';
import { Container, Table } from 'semantic-ui-react';

class MessagesPanel extends Component {
  constructor(props) {
    super(props);
    this.state = {
      messages: null,
    };
  };

  render() {
    return (
      <Container>
        <br />
        <Table celled structured>
          <Table.Header>
            <Table.Row>
              <Table.HeaderCell>Partition</Table.HeaderCell>
              <Table.HeaderCell>Offset</Table.HeaderCell>
              <Table.HeaderCell>Timestamp</Table.HeaderCell>
              <Table.HeaderCell>Key</Table.HeaderCell>
              <Table.HeaderCell>Message</Table.HeaderCell>
            </Table.Row>
          </Table.Header>

          <Table.Body>
            { this.state.messages != null && Object.keys(this.state.messages).map(property => {
              return (
                  this.getCells(property)
              );
            })}
          </Table.Body>
        </Table>
        
      </Container>
    );
  };

  getCells = (property) => {
    if (this.state.messages[property].length === 0) {
      return (
        <Table.Row>
          <Table.Cell>{ property }</Table.Cell>
          <Table.Cell />
          <Table.Cell />
          <Table.Cell />
          <Table.Cell />
        </Table.Row>
      );
    } else {
      //console.log(this.state.messages[property]);
      return (
        this.state.messages[property].map(element => {
          console.log(element);
          return (
            <Table.Row>
              <Table.Cell>{element.partition}</Table.Cell>
              <Table.Cell>{element.offset}</Table.Cell>
              <Table.Cell>{element.timestamp}</Table.Cell>
              <Table.Cell>{element.key}</Table.Cell>
              <Table.Cell>{element.message}</Table.Cell>
            </Table.Row>
          )
        })
      );
    }
  };

  componentDidMount() {
    console.log('Mounted message panel');
    this.getMessages();
  };

  getNumberOfMessages = (numberOfMessages) => {
    if (numberOfMessages === 0) {
      return 1;
    } else {
      return numberOfMessages;
    }
  };

  getMessages = () => {
    fetch('/messages/test-topic')
    .then(response => {
        return response.json()
    })
    .then(message => {
        if (message) {
          this.setState({ messages: message });
        }
    });
  };
};

export default MessagesPanel;