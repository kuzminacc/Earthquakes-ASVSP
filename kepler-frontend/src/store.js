import { createStore, combineReducers, applyMiddleware } from "redux";
import keplerGlReducer from "kepler.gl/reducers";
import { taskMiddleware } from "react-palm/tasks";

// Kombinujemo Kepler reducer sa drugim reducer-ima (ako ih bude)
const reducers = combineReducers({
  keplerGl: keplerGlReducer
});

// Kreiramo store sa task middleware-om
const store = createStore(reducers, {}, applyMiddleware(taskMiddleware)); //depricated

export default store;
