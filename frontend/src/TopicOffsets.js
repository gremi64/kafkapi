import React, { Component } from 'react';

import { Icon, Table, Progress, Container, Header } from 'semantic-ui-react'

class TopicOffsets extends Component {
  constructor(props) {
    super(props);
  }

  getColor(percent) {
    if (percent < 25) {
      return 'red';
    } else if (percent < 95) {
      return 'orange';
    } else {
      return 'green';
    }
  }

  render() {
    return (
      <Container>
        <div>
          <Header as='h2' icon textAlign='center'>
            <Icon name='blind' circular />
            <Header.Content>Topic : {this.props.topic}<br />
              Groupe: {this.props.group}
            </Header.Content>
          </Header>
        </div>
        <Table celled>
          <Table.Header>
            <Table.Row>
              <Table.HeaderCell>Partition</Table.HeaderCell>
              <Table.HeaderCell>Min Offset</Table.HeaderCell>
              <Table.HeaderCell>Current Offset</Table.HeaderCell>
              <Table.HeaderCell>Max Offset</Table.HeaderCell>
              <Table.HeaderCell>Progress</Table.HeaderCell>
            </Table.Row>
          </Table.Header>

          <Table.Body>
            {this.props.partitionOffsetResult.map(i => {
              const percentage = (i.offset - i.minOffset) / (i.maxOffset - i.minOffset) * 100;
              return (
                <Table.Row key={i.partition}>
                  <Table.Cell>{i.partition}</Table.Cell>
                  <Table.Cell>{i.minOffset}</Table.Cell>
                  <Table.Cell>{i.offset}</Table.Cell>
                  <Table.Cell>{i.maxOffset}</Table.Cell>
                  <Table.Cell>
                    <Progress percent={percentage} color={this.getColor(percentage)}>{i.offset}
                    </Progress>
                  </Table.Cell>
                </Table.Row>
              )
            })}
          </Table.Body>
        </Table>
      </Container>
    );
  }


}


export default TopicOffsets;
