import React, { Component } from 'react';
import moment from 'moment';
import { Container, Table, Divider, Header } from 'semantic-ui-react';

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
        { this.state.messages != null && Object.keys(this.state.messages).map(property => {
          return (
            this.getCells(property)
          );
        })}
      </Container>
    );
  };

  componentDidMount() {
    console.log('Mounted message panel');
    this.getMessages();
  };

  getCells = (property) => {
    if (this.state.messages[property].length === 0) {
      return (
        this.getEmptyTable(property)
      );
    } else {
      return (
        this.getTableWithData(property)
      );
    }
  };

  getEmptyTable = (property) => {
    return (
      <Container key={ property }>
        <Divider hidden/><Divider />
        <Header as='h3'>Partition: { property }</Header>
        <Table celled textAlign='center'>
          <Table.Header>
            <Table.Row>
              <Table.HeaderCell width={2}>Offset</Table.HeaderCell>
              <Table.HeaderCell width={3}>Timestamp</Table.HeaderCell>
              <Table.HeaderCell width={2}>Key</Table.HeaderCell>
              <Table.HeaderCell>Message</Table.HeaderCell>
            </Table.Row>
          </Table.Header>

          <Table.Body>
          </Table.Body>
        </Table>
      </Container>
    )
  };

  getTableWithData = (property) => {
    return (
      <Container key={ property }>
        <Divider hidden/><Divider hidden={ property === '0' }/>
        <Header as='h3'>Partition: { property }</Header>
        <Table celled textAlign='center'>
          <Table.Header>
            <Table.Row>
              <Table.HeaderCell width={2}>Offset</Table.HeaderCell>
              <Table.HeaderCell width={3}>Timestamp</Table.HeaderCell>
              <Table.HeaderCell width={2}>Key</Table.HeaderCell>
              <Table.HeaderCell>Message</Table.HeaderCell>
            </Table.Row>
          </Table.Header>

          <Table.Body>
          { this.state.messages[property].map(element => {
            return (
              <Table.Row key={ element.offset }>
                <Table.Cell>{ element.offset }</Table.Cell>
                <Table.Cell>{ moment(element.timestamp).format("YYYY-MM-DD HH:mm") }</Table.Cell>
                <Table.Cell>{ element.key }</Table.Cell>
                <Table.Cell textAlign='left'>{ element.message }</Table.Cell>
              </Table.Row>
            )
          })}
          </Table.Body>
        </Table>
      </Container>
    );
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