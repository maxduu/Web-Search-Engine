import 'react-bulma-components/dist/react-bulma-components.min.css';
import { BrowserRouter as Router, Route } from 'react-router-dom';

import * as ROUTES from './constants/routes';

import SearchPage from './pages/searchpage';
import ResultsPage from './pages/resultspage';

const App = () => (
  <Router>
    <Route exact path={ROUTES.SEARCH} component={SearchPage} />
    <Route exact path={ROUTES.RESULTS} component={ResultsPage} />
  </Router>
);

export default App;
