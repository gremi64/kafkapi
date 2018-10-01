import React, { Component } from 'react';
import { Container, Form, Divider } from 'semantic-ui-react';

class MessagesForm extends Component {

  render() {
    return (
      <Container textAlign='center'>
        <Divider hidden />
        <Form onSubmit={ () => { this.props.onFormSubmit(this.state.selectedTopic, this.state.selectedGroup, this.state.selectedBrokers, this.state.selectedSecurity) } }>
          <Form.Group widths='equal'>
            <Form.Input fluid
              id='myTopic' 
              label='Topic'
              onChange={ (event) => { this.setState({selectedTopic: event.target.value})} }
            />
            <Form.Input fluid
              id='myGroup' 
              label='Group'
              onChange={ (event) => { this.setState({selectedGroup: event.target.value})} }
            />
          </Form.Group>
          <Form.Group widths='equal'>
            <Form.Dropdown  fluid  search selection 
              placeholder='Select brokers'
              options={ this.props.brokers } 
              onChange={ (event, data) => this.handleDropdownChange(event, data, 'selectedBrokers') } />
            <Form.Dropdown fluid search selection 
              placeholder='Select security' 
              options={ this.props.securities } 
              onChange={ (event, data) => this.handleDropdownChange(event, data, 'selectedSecurity') } />
          </Form.Group>
          <Form.Button content='Submit' />
        </Form>
      </Container>
    );
  };

  handleDropdownChange = (event, data, field) => {
    const { value } = data;
    const { key } = data.options.find(o => o.value === value);   
    this.setState({[field]: key});
  };
};

export default MessagesForm;
