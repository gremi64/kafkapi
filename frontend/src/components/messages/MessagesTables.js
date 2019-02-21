import React from 'react';
import moment from 'moment';
import { Container, Table, Divider, Header, Dimmer, Loader } from 'semantic-ui-react';

function getEmptyTable(property) {
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

function getTableWithData(property, props) {
  return (
    <Container key={ property }>
      <Divider hidden={ property === '0' }/>
      <Header as='h3'>Partition: { property }</Header>
      <Table celled textAlign='center'
      pagination
      paginationMaxRowsPerPage={20}
      >
        <Table.Header>
          <Table.Row>
            <Table.HeaderCell width={2}>Offset</Table.HeaderCell>
            <Table.HeaderCell width={3}>Timestamp</Table.HeaderCell>
            <Table.HeaderCell width={2}>Key</Table.HeaderCell>
            <Table.HeaderCell>Message</Table.HeaderCell>
          </Table.Row>
        </Table.Header>

        <Table.Body>
        { props.messages[property].map(element => {
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

function getCells(property, props) {
  if (props.messages[property].length === 0) {
    return (
      getEmptyTable(property)
    );
  } else {
    return (
      getTableWithData(property, props)
    );
  }
};

const MessagesTables = (props) => {
  return (
    <Container className='dimmable'>
      <Dimmer inverted active={props.loadingOffset}>
        <Loader />
      </Dimmer>
      { props.messages != null && Object.keys(props.messages).map(property => {
        return (
          getCells(property, props)
        );
      })}
    </Container>
  );
};

export default MessagesTables;
