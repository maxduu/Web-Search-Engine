import React, { useState } from 'react';
import Container from 'react-bulma-components/lib/components/container';
import Image from 'react-bulma-components/lib/components/image';
import Columns from 'react-bulma-components/lib/components/columns';
import { Field, Input } from 'react-bulma-components/lib/components/form';
import Button from 'react-bulma-components/lib/components/button';
import Hero from 'react-bulma-components/lib/components/hero';

import * as ROUTES from '../constants/routes';
const logo = '../../walmart-google.png';

const SearchPage = () => {
  const [searchTerm, setSearchTerm] = useState('');

  const onSearchClick = () => {
    window.location = `${ROUTES.RESULTS}?search=${searchTerm} `;
  };

  return (
    <Columns>
      <Columns.Column size={3} />
      <Columns.Column size={6}>
        <Hero size='medium'>
          <Hero.Body>
            <Container>
              <a href={ROUTES.SEARCH}>
                <Image src={logo} />
              </a>
              <br />
              <Field kind='addons'>
                <Input
                  placeholder='Search'
                  onChange={(e) => setSearchTerm(e.target.value)}
                  value={searchTerm}
                />
                <Button
                  renderAs='button'
                  onClick={onSearchClick}
                  disabled={!searchTerm.trim().length}>
                  Search
                </Button>
              </Field>
            </Container>
          </Hero.Body>
        </Hero>
      </Columns.Column>
      <Columns.Column size={3} />
    </Columns>
  );
};

export default SearchPage;
