import React from 'react';

import { Table, Progress, Container  } from 'semantic-ui-react'

function getColor(percent) {
  if (percent === 0 || isNaN(percent)) {
    return 'grey';
  } else if (percent < 25) {
    return 'red';
  } else if (percent < 95) {
    return 'orange';
  } else {
    return 'green';
  }
}

const TopicOffsets = (props) => {
  return (
    <Container>
      <Table celled textAlign='center'>
        <Table.Header>
          <Table.Row>
            <Table.HeaderCell>Partition</Table.HeaderCell>
            <Table.HeaderCell>Min Offset</Table.HeaderCell>
            <Table.HeaderCell>Current Offset</Table.HeaderCell>
            <Table.HeaderCell>Max Offset</Table.HeaderCell>
          </Table.Row>
        </Table.Header>

        <Table.Body>
          {props.partitionOffsetResult.map(i => {
            const percentage = (i.offset - i.minOffset) / (i.maxOffset - i.minOffset) * 100;
            return (
              <Table.Row key={i.partition}>
                <Table.Cell>{i.partition}</Table.Cell>
                <Table.Cell>{i.minOffset}</Table.Cell>
                <Table.Cell>
                  <Progress percent={percentage} color={getColor(percentage)}>{
                      (() => {
                        if (i.offset != null && isFinite(i.offset)) {
                          return i.offset;
                        } else {
                          return "No commited offset";
                        }
                      })()
                    }
                  </Progress>
                </Table.Cell>
                <Table.Cell>{i.maxOffset}</Table.Cell>
              </Table.Row>
            )
          })}
        </Table.Body>
      </Table>
    </Container>
  );
}

export default TopicOffsets;
