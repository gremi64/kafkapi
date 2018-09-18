import React, { Component } from 'react';

import { Icon, Label, Menu, Table } from 'semantic-ui-react'
import { Progress, Container } from 'semantic-ui-react'
import { Header, Image } from 'semantic-ui-react'

class TopicOffsets extends Component {

  state = {
    result: {
      "topic": "he_dev_executableOperation_priv_v1",
      "group": "he_dev_u5_executableOperation",
      "partitionOffsetResult": [
        {
          "partition": 0,
          "offset": 12,
          "minOffset": 0,
          "maxOffset": 100

        },
        {
          "partition": 1,
          "offset": 75,
          "minOffset": 0,
          "maxOffset": 100
        },
        {
          "partition": 2,
          "offset": 148,
          "minOffset": 100,
          "maxOffset": 150
        }
      ]
    }
  };

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
            <Header.Content>Topic : {this.state.result.topic}<br />
              Groupe: {this.state.result.group}
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
            {this.state.result.partitionOffsetResult.map(i => {
              const percentage = (i.offset - i.minOffset) / (i.maxOffset - i.minOffset) * 100;
              return (
                <Table.Row>
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
