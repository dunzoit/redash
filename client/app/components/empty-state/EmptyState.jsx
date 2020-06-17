import {keys, some} from "lodash";
import React, {useCallback} from "react";
import PropTypes from "prop-types";
import classNames from "classnames";
import CreateDashboardDialog from "@/components/dashboards/CreateDashboardDialog";
import {currentUser} from "@/services/auth";
import organizationStatus from "@/services/organizationStatus";
import "./empty-state.less";

function Step({show, completed, text, url, urlText, onClick}) {
  if (!show) {
    return null;
  }

  return (
    <li className={classNames({done: completed})}>
      <a href={url} onClick={onClick}>
        {urlText}
      </a>{" "}
      {text}
    </li>
  );
}

Step.propTypes = {
  show: PropTypes.bool.isRequired,
  completed: PropTypes.bool.isRequired,
  text: PropTypes.string.isRequired,
  url: PropTypes.string,
  urlText: PropTypes.string,
  onClick: PropTypes.func,
};

Step.defaultProps = {
  url: null,
  urlText: null,
  onClick: null,
};

function EmptyState({
                      icon,
                      header,
                      description,
                      illustration,
                      helpLink,
                      onboardingMode,
                      showAlertStep,
                      showDashboardStep,
                      showInviteStep,
                    }) {
  const isAvailable = {
    dataSource: true,
    query: true,
    alert: showAlertStep,
    dashboard: showDashboardStep,
    inviteUsers: showInviteStep,
  };

  const isCompleted = {
    dataSource: organizationStatus.objectCounters.data_sources > 0,
    query: organizationStatus.objectCounters.queries > 0,
    alert: organizationStatus.objectCounters.alerts > 0,
    dashboard: organizationStatus.objectCounters.dashboards > 0,
    inviteUsers: organizationStatus.objectCounters.users > 1,
  };

  const showCreateDashboardDialog = useCallback(() => {
    CreateDashboardDialog.showModal();
  }, []);

  // Show if `onboardingMode=false` or any requested step not completed
  const shouldShow = !onboardingMode || some(keys(isAvailable), step => isAvailable[step] && !isCompleted[step]);

  if (!shouldShow) {
    return null;
  }

  return (
    <div className="empty-state bg-white tiled">
      <div className="empty-state__summary">
        {header && <h4>{header}</h4>}
        <h2>
          <i className={icon}/>
        </h2>
        <p>{description}</p>
        <p>
          <ol>
            <li>
              <b>
                This is an upgraded and improved version of Redash. Click on the bottom left button to expand the menu
                bar. You can now see data types and partitions in athena table schemas. Alerts are much easier to set up
                and you can configure the alert message now.
              </b>
            </li>
            <li>
              Redash is primarily meant as a reporting and exploration tool. It is NOT meant as a way to query
              raw data and download it as CSV for further processing. Please aim to do all aggregations and slicing
              and dicing in the query itself. Redash queries will FAIL if you try to download too many rows ~10 lac.
              Use a LIMIT filter in your queries if you want to explore.
            </li>
            <li>Do not name your saved queries with these patterns. Any such saved query older than 7 days
              will be removed from the system.
              <ul>
                <li>
                  New Query
                </li>
                <li>
                  Test Query
                </li>
                <li>
                  test_query
                </li>
                <li>
                  Copy of any text
                </li>
              </ul>
            </li>
            <li>
              Please add multiple tags to your saved queries. A tag with your name is helpful in filtering queries by
              user.
            </li>
            <li>
              There is query timeout of 10 minutes in all postgres datasources. Please write performant queries
              accordingly.
            </li>
            <li>
              While downloading data as csv or excel files, you may see a popup. Please click 'Leave'. You will not be
              redirected anywhere. Also if you have a large number of rows, the data download may fail. You will have to
              reduce the number of rows in that case.
            </li>
            <li>
              As with any tool, Redash also has some limitations but we have tried to make it as smooth as possible for
              users.
              Please provide us with feedback on how we can improve it further.
            </li>
          </ol>
        </p>
        <img
          src={"/static/images/illustrations/" + illustration + ".svg"}
          alt={illustration + " Illustration"}
          width="75%"
        />
      </div>
      {/*<div className="empty-state__steps">*/}
      {/*  <h4>Let&apos;s get started</h4>*/}
      {/*  <ol>*/}
      {/*    {currentUser.isAdmin && (*/}
      {/*      <Step*/}
      {/*        show={isAvailable.dataSource}*/}
      {/*        completed={isCompleted.dataSource}*/}
      {/*        url="data_sources/new"*/}
      {/*        urlText="Connect"*/}
      {/*        text="a Data Source"*/}
      {/*      />*/}
      {/*    )}*/}
      {/*    {!currentUser.isAdmin && (*/}
      {/*      <Step*/}
      {/*        show={isAvailable.dataSource}*/}
      {/*        completed={isCompleted.dataSource}*/}
      {/*        text="Ask an account admin to connect a data source"*/}
      {/*      />*/}
      {/*    )}*/}
      {/*    <Step*/}
      {/*      show={isAvailable.query}*/}
      {/*      completed={isCompleted.query}*/}
      {/*      url="queries/new"*/}
      {/*      urlText="Create"*/}
      {/*      text="your first Query"*/}
      {/*    />*/}
      {/*    <Step*/}
      {/*      show={isAvailable.alert}*/}
      {/*      completed={isCompleted.alert}*/}
      {/*      url="alerts/new"*/}
      {/*      urlText="Create"*/}
      {/*      text="your first Alert"*/}
      {/*    />*/}
      {/*    <Step*/}
      {/*      show={isAvailable.dashboard}*/}
      {/*      completed={isCompleted.dashboard}*/}
      {/*      onClick={showCreateDashboardDialog}*/}
      {/*      urlText="Create"*/}
      {/*      text="your first Dashboard"*/}
      {/*    />*/}
      {/*    <Step*/}
      {/*      show={isAvailable.inviteUsers}*/}
      {/*      completed={isCompleted.inviteUsers}*/}
      {/*      url="users/new"*/}
      {/*      urlText="Invite"*/}
      {/*      text="your team members"*/}
      {/*    />*/}
      {/*  </ol>*/}
      {/*  <p>*/}
      {/*    Need more support?{" "}*/}
      {/*    <a href={helpLink} target="_blank" rel="noopener noreferrer">*/}
      {/*      See our Help*/}
      {/*      <i className="fa fa-external-link m-l-5" aria-hidden="true" />*/}
      {/*    </a>*/}
      {/*  </p>*/}
      {/*</div>*/}
    </div>
  );
}

EmptyState.propTypes = {
  icon: PropTypes.string,
  header: PropTypes.string,
  description: PropTypes.string.isRequired,
  illustration: PropTypes.string.isRequired,
  helpLink: PropTypes.string.isRequired,

  onboardingMode: PropTypes.bool,
  showAlertStep: PropTypes.bool,
  showDashboardStep: PropTypes.bool,
  showInviteStep: PropTypes.bool,
};

EmptyState.defaultProps = {
  icon: null,
  header: null,

  onboardingMode: false,
  showAlertStep: false,
  showDashboardStep: false,
  showInviteStep: false,
};

export default EmptyState;
